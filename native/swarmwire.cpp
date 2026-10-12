// SwarmWire: a bounded, event-driven local message router. MIT license.
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using Bytes = std::vector<unsigned char>;
using Body = std::shared_ptr<const Bytes>;
constexpr size_t MAX_FRAME = 1024 * 1024 + 256;
constexpr size_t PEER_BYTES = 4 * 1024 * 1024;
constexpr size_t GLOBAL_BYTES = 128 * 1024 * 1024;
constexpr size_t BLOB_BYTES = 64 * 1024 * 1024;
constexpr unsigned HELLO=1, SEND=2, CREDIT=3, STORE=4, LOAD=5, STATS=6;
constexpr unsigned SEND_STREAM=7;
constexpr unsigned SEND_STREAM_CREDIT=8, SEND_CREDIT=9;
constexpr unsigned REGISTER_ROUTE=10, OPEN_ROUTE=11, ACCEPT_ROUTE=12;
constexpr unsigned ACK=128, DELIVERY=129, ERROR=130;
static volatile sig_atomic_t running = 1;
static void stop(int) { running = 0; }
static uint32_t u32(const unsigned char* p) { uint32_t n; std::memcpy(&n,p,4); return ntohl(n); }
static uint16_t u16(const unsigned char* p) { uint16_t n; std::memcpy(&n,p,2); return ntohs(n); }
static void add32(Bytes& b, uint32_t n) { n=htonl(n); auto p=reinterpret_cast<unsigned char*>(&n); b.insert(b.end(),p,p+4); }
static void add16(Bytes& b, uint16_t n) { n=htons(n); auto p=reinterpret_cast<unsigned char*>(&n); b.insert(b.end(),p,p+2); }
static Body body(const std::string& s) { return std::make_shared<const Bytes>(s.begin(),s.end()); }
static Body empty() { static Body b=body(""); return b; }

struct Packet {
    Bytes header; Body data; size_t offset=0;
    Packet(unsigned op, uint32_t id, const std::string& name, Body b): data(std::move(b)) {
        add32(header,static_cast<uint32_t>(7+name.size()+data->size()));
        header.push_back(static_cast<unsigned char>(op)); add32(header,id);
        add16(header,static_cast<uint16_t>(name.size())); header.insert(header.end(),name.begin(),name.end());
    }
    size_t size() const { return header.size()+data->size(); }
};
struct Event { int fd; bool read; bool write; };
class Reactor {
    int fd;
public:
    Reactor() {
#ifdef __linux__
        fd=epoll_create1(EPOLL_CLOEXEC);
#else
        fd=kqueue();
#endif
        if(fd<0) throw std::runtime_error("cannot create reactor");
    }
    ~Reactor() { ::close(fd); }
    void add(int s) {
#ifdef __linux__
        epoll_event e{}; e.data.fd=s; e.events=EPOLLIN|EPOLLRDHUP;
        if(epoll_ctl(fd,EPOLL_CTL_ADD,s,&e)<0) throw std::runtime_error("epoll add failed");
#else
        struct kevent e; EV_SET(&e,s,EVFILT_READ,EV_ADD,0,0,nullptr);
        if(kevent(fd,&e,1,nullptr,0,nullptr)<0) throw std::runtime_error("kqueue add failed");
#endif
    }
    void writable(int s, bool on) {
#ifdef __linux__
        epoll_event e{}; e.data.fd=s; e.events=EPOLLIN|EPOLLRDHUP|(on?EPOLLOUT:0);
        if(epoll_ctl(fd,EPOLL_CTL_MOD,s,&e)<0) throw std::runtime_error("epoll modify failed");
#else
        struct kevent e; EV_SET(&e,s,EVFILT_WRITE,on?EV_ADD:EV_DELETE,0,0,nullptr);
        if(kevent(fd,&e,1,nullptr,0,nullptr)<0 && errno!=ENOENT) throw std::runtime_error("kqueue modify failed");
#endif
    }
    std::vector<Event> wait() {
        std::vector<Event> out;
#ifdef __linux__
        epoll_event events[256]; int n=epoll_wait(fd,events,256,-1);
        for(int i=0;i<n;++i) out.push_back({events[i].data.fd,bool(events[i].events&(EPOLLIN|EPOLLERR|EPOLLHUP|EPOLLRDHUP)),bool(events[i].events&EPOLLOUT)});
#else
        struct kevent events[256]; int n=kevent(fd,nullptr,0,events,256,nullptr);
        for(int i=0;i<n;++i) out.push_back({static_cast<int>(events[i].ident),events[i].filter==EVFILT_READ,events[i].filter==EVFILT_WRITE});
#endif
        if(n<0 && errno!=EINTR) throw std::runtime_error("reactor wait failed");
        return out;
    }
};
struct Peer {
    int fd; std::string name, route; Bytes input; size_t cursor=0, queued=0;
    unsigned credits=0, capacity=0; bool write_on=false;
    std::deque<Packet> output, pending;
    explicit Peer(int s):fd(s) { input.reserve(4096); }
};
struct Blob { Body data; std::list<std::string>::iterator lru; };
struct Ticket { int source, destination; std::chrono::steady_clock::time_point issued; };
class Broker {
    Reactor reactor; int listener=-1, lifetime=-1; std::string path, token;
    std::unordered_map<int,std::unique_ptr<Peer>> peers;
    std::unordered_map<std::string,int> names;
    std::unordered_set<int> dirty;
    std::unordered_map<std::string,Blob> blobs; std::list<std::string> lru;
    std::unordered_map<std::string,Ticket> tickets;
    size_t queued=0, blob_bytes=0; uint64_t routed=0, rejected=0;
    void drop(int fd) {
        auto it=peers.find(fd); if(it==peers.end()) return;
        auto& p=*it->second; if(!p.name.empty()) names.erase(p.name);
        for(auto ticket=tickets.begin();ticket!=tickets.end();) {
            if(ticket->second.source==fd || ticket->second.destination==fd) ticket=tickets.erase(ticket);
            else ++ticket;
        }
        queued-=p.queued; ::close(fd); peers.erase(it); dirty.erase(fd);
    }
    void enqueue(Peer& p, Packet packet, bool pending=false) {
        const size_t n=packet.size();
        if(p.queued+n>PEER_BYTES || queued+n>GLOBAL_BYTES) throw std::runtime_error("output budget exhausted");
        p.queued+=n; queued+=n;
        (pending?p.pending:p.output).push_back(std::move(packet)); dirty.insert(p.fd);
    }
    void ack(Peer& p, uint32_t id, Body data=empty()) { enqueue(p,Packet(ACK,id,"",std::move(data))); }
    void error(Peer& p, uint32_t id, const std::string& why) { ++rejected; enqueue(p,Packet(ERROR,id,"",body(why))); }
    void release(Peer& p) {
        while(p.credits && !p.pending.empty()) {
            --p.credits; p.output.push_back(std::move(p.pending.front())); p.pending.pop_front(); dirty.insert(p.fd);
        }
    }
    static bool valid_name(const std::string& s) {
        return !s.empty() && s.size()<=64 && std::all_of(s.begin(),s.end(),[](unsigned char c){return (c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_'||c=='-'||c=='.';});
    }
    void command(Peer& p, unsigned op, uint32_t id, const std::string& target, Body data) {
        if(op==HELLO) {
            if(!p.name.empty() || !valid_name(target) || names.count(target) || data->size()<4) { error(p,id,"invalid or duplicate agent identity"); return; }
            unsigned cap=u32(data->data());
            std::string supplied(data->begin()+4,data->end());
            if(cap<1 || cap>4096 || supplied!=token) { error(p,id,"invalid capacity or authentication"); return; }
            p.name=target; p.capacity=cap; p.credits=cap; names[target]=p.fd;
            // Capability bit 0: a send can also return receiver credits.
            ack(p,id,std::make_shared<const Bytes>(Bytes{1})); return;
        }
        if(p.name.empty()) { error(p,id,"register first"); return; }
        if(op==REGISTER_ROUTE) {
            std::string address(data->begin(),data->end());
            if(address.empty() || address.size()>100 || (address.rfind("unix:/",0)!=0 && address.rfind("tcp:127.0.0.1:",0)!=0)) {
                error(p,id,"invalid local peer address"); return;
            }
            p.route=address; ack(p,id); return;
        }
        if(op==OPEN_ROUTE) {
            auto it=names.find(target);
            if(it==names.end() || peers.at(it->second)->route.empty()) { error(p,id,"peer route unavailable"); return; }
            auto now=std::chrono::steady_clock::now();
            for(auto ticket=tickets.begin();ticket!=tickets.end();) {
                if(now-ticket->second.issued>std::chrono::seconds(10)) ticket=tickets.erase(ticket);
                else ++ticket;
            }
            if(tickets.size()>=4096) { error(p,id,"BACKPRESSURE: route ticket limit"); return; }
            static std::random_device random;
            static const char hex[]="0123456789abcdef";
            std::string key;
            do {
                key.clear(); key.reserve(32);
                for(int n=0;n<4;++n) { auto value=static_cast<uint32_t>(random()); for(int shift=28;shift>=0;shift-=4) key.push_back(hex[(value>>shift)&15]); }
            } while(tickets.count(key));
            tickets.emplace(key,Ticket{p.fd,it->second,now});
            ack(p,id,body(peers.at(it->second)->route+"\n"+key)); return;
        }
        if(op==ACCEPT_ROUTE) {
            auto ticket=tickets.find(target);
            if(ticket==tickets.end() || ticket->second.destination!=p.fd || !peers.count(ticket->second.source) || std::chrono::steady_clock::now()-ticket->second.issued>std::chrono::seconds(10)) {
                error(p,id,"invalid or expired route ticket"); return;
            }
            std::string source=peers.at(ticket->second.source)->name;
            tickets.erase(ticket); ack(p,id,body(source)); return;
        }
        if(op==SEND || op==SEND_STREAM) {
            auto it=names.find(target);
            if(it==names.end()) { error(p,id,"unknown recipient"); return; }
            auto& dest=*peers.at(it->second); Packet packet(DELIVERY,0,p.name,std::move(data));
            const size_t n=packet.size();
            // Reserve acknowledgement space before accepting a message; no silent partial acceptance.
            if(dest.pending.size()+dest.output.size()>=4096 || dest.queued+n+11>PEER_BYTES || p.queued+11+(p.fd==dest.fd?n:0)>PEER_BYTES || queued+n+11>GLOBAL_BYTES) {
                error(p,id,"BACKPRESSURE: recipient or broker queue is full"); return;
            }
            enqueue(dest,std::move(packet),true); release(dest); ++routed;
            if(op==SEND) ack(p,id);
            return;
        }
        if(op==CREDIT) {
            if(data->size()!=4) throw std::runtime_error("invalid credit frame");
            unsigned n=u32(data->data());
            if(n>p.capacity-p.credits) throw std::runtime_error("invalid credit grant");
            p.credits+=n; release(p); return;
        }
        if(op==STORE) {
            if(target.size()!=64 || !std::all_of(target.begin(),target.end(),[](char c){return (c>='0'&&c<='9')||(c>='a'&&c<='f');})) { error(p,id,"invalid blob key"); return; }
            auto it=blobs.find(target);
            if(it!=blobs.end()) {
                if(*it->second.data!=*data) { error(p,id,"blob key collision"); return; }
                lru.splice(lru.end(),lru,it->second.lru); ack(p,id); return;
            }
            while(blob_bytes+data->size()>BLOB_BYTES && !lru.empty()) {
                auto old=blobs.find(lru.front()); blob_bytes-=old->second.data->size(); blobs.erase(old); lru.pop_front();
            }
            lru.push_back(target); blob_bytes+=data->size(); blobs.emplace(target,Blob{std::move(data),std::prev(lru.end())}); ack(p,id); return;
        }
        if(op==LOAD) {
            auto it=blobs.find(target); if(it==blobs.end()) { error(p,id,"blob missing or evicted"); return; }
            if(p.queued+it->second.data->size()+11>PEER_BYTES || queued+it->second.data->size()+11>GLOBAL_BYTES) { error(p,id,"BACKPRESSURE: output queue is full"); return; }
            lru.splice(lru.end(),lru,it->second.lru); ack(p,id,it->second.data); return;
        }
        if(op==STATS) {
            ack(p,id,body("{\"agents\":"+std::to_string(names.size())+",\"routed\":"+std::to_string(routed)+",\"rejected\":"+std::to_string(rejected)+",\"queued_bytes\":"+std::to_string(queued)+",\"blob_bytes\":"+std::to_string(blob_bytes)+"}")); return;
        }
        error(p,id,"unknown operation");
    }
    bool read(Peer& p) {
        unsigned char buf[65536]; size_t budget=0;
        while(budget<262144) {
            ssize_t n=::recv(p.fd,buf,sizeof(buf),0);
            if(n==0) return false;
            if(n<0) { if(errno==EINTR) continue; return errno==EAGAIN||errno==EWOULDBLOCK; }
            budget+=static_cast<size_t>(n); p.input.insert(p.input.end(),buf,buf+n);
            while(p.input.size()-p.cursor>=4) {
                const unsigned char* ptr=p.input.data()+p.cursor; uint32_t len=u32(ptr);
                if(len<7 || len>MAX_FRAME) return false;
                if(p.input.size()-p.cursor<len+4) break;
                uint16_t ns=u16(ptr+9); if(ns>128 || size_t(7+ns)>len) return false;
                std::string target(reinterpret_cast<const char*>(ptr+11),ns);
                unsigned op=ptr[4]; size_t start=11+ns;
                if(op==SEND_STREAM_CREDIT || op==SEND_CREDIT) {
                    if(p.name.empty() || start+4>len+4) return false;
                    unsigned grant=u32(ptr+start);
                    if(grant>p.capacity-p.credits) return false;
                    p.credits+=grant; release(p); start+=4;
                    op=op==SEND_STREAM_CREDIT?SEND_STREAM:SEND;
                }
                Body payload=std::make_shared<const Bytes>(ptr+start,ptr+4+len);
                command(p,op,u32(ptr+5),target,std::move(payload)); p.cursor+=len+4;
            }
            if(p.cursor) { p.input.erase(p.input.begin(),p.input.begin()+static_cast<long>(p.cursor)); p.cursor=0; }
        }
        return true;
    }
    bool flush(Peer& p) {
        size_t budget=0;
        while(!p.output.empty() && budget<262144) {
            iovec vec[64]; int count=0;
            for(auto& packet:p.output) {
                size_t pos=packet.offset;
                if(pos<packet.header.size()) vec[count++]={packet.header.data()+pos,packet.header.size()-pos};
                pos=pos>packet.header.size()?pos-packet.header.size():0;
                if(pos<packet.data->size()) vec[count++]={const_cast<unsigned char*>(packet.data->data()+pos),packet.data->size()-pos};
                if(count>=62) break;
            }
            ssize_t n=writev(p.fd,vec,count);
            if(n<0) { if(errno==EINTR) continue; if(errno==EAGAIN||errno==EWOULDBLOCK) break; return false; }
            if(n==0) return false;
            size_t used=static_cast<size_t>(n); budget+=used; p.queued-=used; queued-=used;
            while(used) {
                auto& packet=p.output.front(); size_t take=std::min(used,packet.size()-packet.offset);
                packet.offset+=take; used-=take; if(packet.offset==packet.size()) p.output.pop_front();
            }
        }
        bool on=!p.output.empty(); if(on!=p.write_on) { reactor.writable(p.fd,on); p.write_on=on; }
        return true;
    }
public:
    Broker(int port, const std::string& unix_path, int lifetime_fd=-1):lifetime(lifetime_fd),path(unix_path) {
        const char* t=std::getenv("SWARMWIRE_TOKEN"); if(t) token=t;
        listener=socket(path.empty()?AF_INET:AF_UNIX,SOCK_STREAM,0);
        if(listener<0) throw std::runtime_error("socket failed");
        if(path.empty()) {
            int yes=1; setsockopt(listener,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
            sockaddr_in a{}; a.sin_family=AF_INET; a.sin_port=htons(static_cast<uint16_t>(port)); a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
            if(bind(listener,reinterpret_cast<sockaddr*>(&a),sizeof(a))<0) throw std::runtime_error("bind failed");
        } else {
            sockaddr_un a{}; a.sun_family=AF_UNIX;
            if(path.size()>=sizeof(a.sun_path)) throw std::runtime_error("Unix socket path too long");
            std::strcpy(a.sun_path,path.c_str());
            if(bind(listener,reinterpret_cast<sockaddr*>(&a),sizeof(a))<0) throw std::runtime_error("Unix bind failed; existing paths are never overwritten");
        }
        if(listen(listener,512)<0) throw std::runtime_error("listen failed");
        fcntl(listener,F_SETFL,O_NONBLOCK); reactor.add(listener);
        if(lifetime>=0) { fcntl(lifetime,F_SETFL,O_NONBLOCK); reactor.add(lifetime); }
        if(path.empty()) { sockaddr_in a{}; socklen_t n=sizeof(a); getsockname(listener,reinterpret_cast<sockaddr*>(&a),&n); std::cout<<"{\"port\":"<<ntohs(a.sin_port)<<"}"<<std::endl; }
        else std::cout<<"{\"ready\":true}"<<std::endl;
    }
    ~Broker() { for(auto& entry:peers) ::close(entry.first); if(listener>=0) ::close(listener); if(lifetime>=0) ::close(lifetime); if(!path.empty()) ::unlink(path.c_str()); }
    void run() {
        while(running) {
            auto events=reactor.wait();
            for(auto e:events) {
                if(e.fd==lifetime) { char data; if(::read(lifetime,&data,1)==0) running=0; continue; }
                if(e.fd==listener) {
                    for(int i=0;i<64;++i) {
                        int s=accept(listener,nullptr,nullptr); if(s<0) break;
                        if(peers.size()>=1024) { ::close(s); continue; }
                        fcntl(s,F_SETFL,O_NONBLOCK); int yes=1;
                        if(path.empty()) setsockopt(s,IPPROTO_TCP,TCP_NODELAY,&yes,sizeof(yes));
                        peers.emplace(s,std::make_unique<Peer>(s)); reactor.add(s);
                    }
                    continue;
                }
                auto it=peers.find(e.fd); if(it==peers.end()) continue;
                try { if(e.read && !read(*it->second)) { drop(e.fd); continue; } if(e.write) dirty.insert(e.fd); }
                catch(const std::exception&) { drop(e.fd); }
            }
            auto ready=std::move(dirty); dirty.clear();
            for(int fd:ready) { auto it=peers.find(fd); if(it==peers.end()) continue; try { if(!flush(*it->second)) drop(fd); } catch(const std::exception&) { drop(fd); } }
        }
    }
};

// Native benchmark uses the same wire format and no JSON or Python.
static void exact(int fd, void* out, size_t n) { auto p=static_cast<unsigned char*>(out); while(n) { ssize_t k=::recv(fd,p,n,0); if(k<=0) throw std::runtime_error("benchmark read failed"); p+=k; n-=static_cast<size_t>(k); } }
static void send_packet(int fd, unsigned op,uint32_t id,const std::string& target,Body data) {
    Packet p(op,id,target,std::move(data)); Bytes bytes=p.header; bytes.insert(bytes.end(),p.data->begin(),p.data->end()); size_t pos=0;
    while(pos<bytes.size()) { ssize_t n=::send(fd,bytes.data()+pos,bytes.size()-pos,0); if(n<=0) throw std::runtime_error("benchmark write failed"); pos+=static_cast<size_t>(n); }
}
static void receive(int fd, unsigned expected) { unsigned char h[4]; exact(fd,h,4); uint32_t n=u32(h); if(n<7||n>MAX_FRAME) throw std::runtime_error("bad frame"); Bytes b(n); exact(fd,b.data(),n); if(b[0]!=expected) throw std::runtime_error("unexpected benchmark response"); }
static int connect_peer(int port,const std::string& path,const std::string& name) {
    int fd=socket(path.empty()?AF_INET:AF_UNIX,SOCK_STREAM,0); int rc;
    if(path.empty()) { sockaddr_in a{}; a.sin_family=AF_INET; a.sin_port=htons(static_cast<uint16_t>(port)); a.sin_addr.s_addr=htonl(INADDR_LOOPBACK); rc=connect(fd,reinterpret_cast<sockaddr*>(&a),sizeof(a)); int yes=1; setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&yes,sizeof(yes)); }
    else { sockaddr_un a{}; a.sun_family=AF_UNIX; if(path.size()>=sizeof(a.sun_path)) throw std::runtime_error("path too long"); std::strcpy(a.sun_path,path.c_str()); rc=connect(fd,reinterpret_cast<sockaddr*>(&a),sizeof(a)); }
    if(rc<0) throw std::runtime_error("benchmark connect failed");
    Bytes cap; add32(cap,4096); const char* t=std::getenv("SWARMWIRE_TOKEN"); if(t) cap.insert(cap.end(),t,t+std::strlen(t));
    send_packet(fd,HELLO,1,name,std::make_shared<const Bytes>(std::move(cap))); receive(fd,ACK); return fd;
}
static void benchmark(int port,const std::string& path,int samples,int payload) {
    if(samples<1||payload<0||payload>1048576) throw std::runtime_error("invalid benchmark arguments");
    std::string suffix=std::to_string(getpid()); int a=connect_peer(port,path,"native-a-"+suffix), b=connect_peer(port,path,"native-b-"+suffix);
    Body bytes=std::make_shared<const Bytes>(static_cast<size_t>(payload),'x'); std::vector<double> us; Bytes credit; add32(credit,32);
    for(int i=0;i<samples+100;++i) {
        auto start=std::chrono::steady_clock::now();
        send_packet(a,SEND,2,"native-b-"+suffix,bytes); receive(a,ACK); receive(b,DELIVERY);
        send_packet(b,SEND,3,"native-a-"+suffix,bytes); receive(b,ACK); receive(a,DELIVERY);
        if(i>=100) us.push_back(std::chrono::duration<double,std::micro>(std::chrono::steady_clock::now()-start).count());
        if((i+1)%32==0) { send_packet(a,CREDIT,0,"",std::make_shared<const Bytes>(credit)); send_packet(b,CREDIT,0,"",std::make_shared<const Bytes>(credit)); }
    }
    ::close(a); ::close(b); std::sort(us.begin(),us.end());
    auto q=[&](double p){return us[static_cast<size_t>(p*static_cast<double>(us.size()-1))];};
    std::cout<<"{\"mode\":\"native_raw\",\"payload_bytes\":"<<payload<<",\"samples\":"<<samples<<",\"rtt_us_p50\":"<<q(.5)<<",\"rtt_us_p95\":"<<q(.95)<<",\"rtt_us_p99\":"<<q(.99)<<"}"<<std::endl;
}
int main(int argc,char** argv) {
    std::signal(SIGPIPE,SIG_IGN); std::signal(SIGTERM,stop); std::signal(SIGINT,stop);
    int port=8765,samples=2000,payload=64,lifetime=-1; std::string path; bool bench=false;
    try {
        for(int i=1;i<argc;++i) { std::string arg=argv[i]; if(arg=="bench") bench=true; else if(arg=="--port"&&i+1<argc) port=std::stoi(argv[++i]); else if(arg=="--unix"&&i+1<argc) path=argv[++i]; else if(arg=="--lifetime-fd"&&i+1<argc) { lifetime=std::stoi(argv[++i]); if(lifetime<0||fcntl(lifetime,F_GETFD)<0) throw std::runtime_error("invalid lifetime fd"); } else if(arg=="--samples"&&i+1<argc) samples=std::stoi(argv[++i]); else if(arg=="--payload"&&i+1<argc) payload=std::stoi(argv[++i]); else throw std::runtime_error("usage: swarmwired [bench] [--port N | --unix PATH] [--lifetime-fd N] [--samples N] [--payload N]"); }
        if(port<0||port>65535) throw std::runtime_error("invalid port");
        if(bench) benchmark(port,path,samples,payload); else { Broker broker(port,path,lifetime); broker.run(); }
    } catch(const std::exception& e) { std::cerr<<e.what()<<std::endl; return 1; }
}
