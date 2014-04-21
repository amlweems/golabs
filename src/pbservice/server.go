package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk

  am_primary bool
  view viewservice.View
  storage map[string]string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.me != pb.view.Primary && pb.me != pb.view.Backup {
    // we're not a valid server
    reply.Err = ErrWrongServer
    return nil
  }

  var ok bool
  reply.Value, ok = pb.storage[args.Key]
  if ok {
    reply.Err = OK
  } else {
    reply.Err = ErrNoKey
  }

  return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.me != pb.view.Primary && pb.me != pb.view.Backup {
    // we're not a valid server
    reply.Err = ErrWrongServer
  }

  reply.Err = OK
  pb.storage[args.Key] = args.Value

  if pb.am_primary && pb.view.Backup != "" {
    ok := call(pb.view.Backup, "PBServer.Put", args, &reply)
    if ok == false {
      // uh-oh
    }
  }

  return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
  view, ok := pb.vs.Ping(pb.view.Viewnum)
  if ok != nil {
    // uh-oh
  }

  if view.Viewnum != pb.view.Viewnum {
    // transition to a new view
    pb.am_primary = (pb.me == view.Primary)
    pb.view = view
    if pb.am_primary && pb.view.Backup != "" {
      args := &PutArgs{}
      var reply PutReply
      for key, value := range pb.storage {
        args.Key = key
        args.Value = value
        ok := call(pb.view.Backup, "PBServer.Put", args, &reply)
        if ok == false {
          // uh-oh
        }
      }
    }
  }
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)

  pb.storage = map[string]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
