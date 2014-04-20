package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  views []View
  view uint
  pending uint
  ping map[string]uint
  ticks uint
}

func NewView(vs *ViewServer, primary string, backup string) (View, uint) {
    view := new(View)
    view.Viewnum = uint(len(vs.views))
    view.Primary = primary
    view.Backup = backup
    vs.views = append(vs.views, *view)
    return *view, view.Viewnum
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  // some servers are considered dead even when pinging
  alive := true
  defer func(){ if alive { vs.ping[args.Me] = vs.ticks } }()

  if vs.view == 0 {
    reply.View, vs.view = NewView(vs, args.Me, "")
    return nil
  }

  curr := &vs.views[vs.view]
  reply.View = *curr
  
  if args.Me == curr.Primary {
    // primary must ack the current view
    if args.Viewnum == curr.Viewnum {
      curr.Acked = true
    } else {
      alive = false
    }

    if vs.pending != 0 && curr.Acked {
      reply.View = vs.views[vs.pending]
      vs.view = vs.pending
      vs.pending = 0
    }
  } else if args.Me == curr.Backup {
    // backup ping, no work needed
  } else if curr.Backup == "" {
    // add Me as a backup since we don't have one
    _, vs.pending = NewView(vs, curr.Primary, args.Me)
  } else {
    // Me wants to be a backup backup
    NewView(vs, curr.Backup, args.Me)
  }

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  reply.View = vs.views[vs.view]

  return nil
}

func (vs *ViewServer) isDead(server string) bool {
  return (vs.ticks - vs.ping[server] >= DeadPings)
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  defer func() { vs.ticks++ }()

  // don't take action unless primary has acked
  if vs.views[vs.view].Acked == false { return }

  primary := vs.views[vs.view].Primary
  backup := vs.views[vs.view].Backup
  if vs.isDead(primary) {
    // try to find a new backup
    found := false
    for c := range vs.views {
      view := vs.views[c]
      if view.Primary == backup &&
        vs.isDead(view.Backup) == false {
        vs.view = view.Viewnum
        found = true
        break
      }
    }

    if !found {
      _, vs.view = NewView(vs, backup, "")
    }
  }
  if vs.isDead(backup) {
    // kill dead backups
    vs.views[vs.view].Backup = ""
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.ping = map[string]uint{}
  vs.views = make([]View, 1)

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
