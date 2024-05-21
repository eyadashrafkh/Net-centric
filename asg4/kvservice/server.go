package kvservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"sysmonitor"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}

	// Add your declarations here.
	backup    string
	hasBackup bool
	data      map[string]string
	mu        sync.RWMutex
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	// Put the value into the key/value database.
	server.mu.Lock()
	defer server.mu.Unlock()

	if args.IsClient {
		if server.id == server.view.Primary {
			if args.DoHash {
				// If the PutArgs has DoHash set to true, hash the value before storing it.
				previousValue, ok := server.data[args.Key]
				hashedValue := hash(args.Value + previousValue)
				if ok {
					reply.Err = OK
					reply.PreviousValue = previousValue
				} else {
					reply.Err = ErrNoKey
					reply.PreviousValue = ""
				}
				val := fmt.Sprintf("%v", hashedValue)
				server.data[args.Key] = val
				args := &PutArgs{args.Key, val, false, false}
				reply := &PutReply{}
				if server.hasBackup {
					err := call(server.backup, "KVServer.Put", args, reply)
					if err != true {
						return nil
					}
				}
			} else {
				// If the PutArgs has DoHash set to false, store the value as is.
				previousValue, ok := server.data[args.Key]
				if ok {
					reply.Err = OK
					reply.PreviousValue = previousValue
				} else {
					reply.Err = ErrNoKey
					reply.PreviousValue = ""
				}
				server.data[args.Key] = args.Value
				args := &PutArgs{args.Key, args.Value, false, false}
				reply := &PutReply{}
				if server.hasBackup {
					err := call(server.backup, "KVServer.Put", args, reply)
					if err != true {
						return nil
					}
				}
			}
		} else {
			reply.Err = ErrWrongServer
			reply.PreviousValue = ""
		}
	} else {
		// If the PutArgs is not from a client, store the value as is.
		if args.DoHash {
			previousValue, ok := server.data[args.Key]
			hashedValue := hash(args.Value + previousValue)
			if ok {
				reply.Err = OK
				reply.PreviousValue = previousValue
			} else {
				reply.Err = ErrNoKey
				reply.PreviousValue = ""
			}
			val := fmt.Sprintf("%v", hashedValue)
			server.data[args.Key] = val
		} else {
			previousValue, ok := server.data[args.Key]
			if ok {
				reply.Err = OK
				reply.PreviousValue = previousValue
			} else {
				reply.Err = ErrNoKey
				reply.PreviousValue = ""
			}
			server.data[args.Key] = args.Value
		}
	}
	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	server.mu.RLock()
	defer server.mu.RUnlock()

	// Get the value from the key/value database.
	if args.IsClient {
		if server.id == server.view.Primary {
			value, ok := server.data[args.Key]
			if ok {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongServer
			reply.Value = ""
		}
	} else {
		// If the GetArgs is not from a client, return the value as is.
		value, ok := server.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	}
	return nil
}

// ping the viewserver periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)
	if err != nil {
		return
	}
	// Your code here.
	server.view = view
	// Determine the server's role based on the view.
	if server.id == server.view.Primary {
		// If the server is the primary and detects a new backup, handle the data forwarding.
		if server.view.Backup != "" {
			if server.view.Backup != server.backup {
				// If the server is the primary and there is a new backup, update the backup state.
				server.backup = server.view.Backup
				server.hasBackup = true
				// Forward data to the new backup.
				for key, value := range server.data {
					args := &PutArgs{key, value, false, false}
					reply := &PutReply{}
					err := call(server.backup, "KVServer.Put", args, reply)
					if err != true {
						return
					}
				}
			}
		} else {
			// If the server is the primary and there is no backup, reset the backup state.
			server.hasBackup = false
			server.backup = ""
		}

	} else if server.id == server.view.Backup {
		//log.Printf("KVServer(%v) is the Backup\n", server.id)
		// Perform backup-specific tasks if any (e.g., ready to accept data from the primary).
	} else {
		//log.Printf("KVServer(%v) is neither Primary nor Backup\n", server.id)
		// Perform tasks for servers that are neither primary nor backup.
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})

	// Add your server initializations here
	// ==================================
	server.backup = ""
	server.hasBackup = false
	server.data = make(map[string]string)
	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}
