package gonzo

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/ngaut/log"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

type Server struct {
	Backend Backend

	ln net.Listener
	t  tomb.Tomb
}

func NewServerAddr(netname, addr, storePath string) (*Server, error) {
	ln, err := net.Listen(netname, addr)
	if err != nil {
		return nil, err
	}
	return NewServer(ln, storePath)
}

func NewServer(ln net.Listener, path string) (*Server, error) {
	s := &Server{ln: ln}
	if len(path) == 0 {
		log.Info("run mongo in memory")
		s.Backend = NewMemoryBackend(&s.t)
		return s, nil
	}

	log.Info("run mongo in TiKV")
	backend, err := NewTikvBackend(&s.t, path)
	if err != nil {
		return nil, err
	}
	s.Backend = backend
	return s, nil
}

func (s *Server) Start() {
	s.t.Go(s.run)
	log.Infof("gonzodb running pid=%d addr=%q", os.Getpid(), s.ln.Addr())
}

func (s *Server) Wait() error {
	return s.t.Wait()
}

func (s *Server) run() error {
	s.t.Go(func() (err error) {
		var conn net.Conn
		defer s.t.Kill(err)
		for {
			conn, err = s.ln.Accept()
			if err != nil {
				return
			}
			s.t.Go(func() error {
				s.handle(conn)
				return nil
			})
		}
	})
	<-s.t.Dying()
	s.ln.Close()
	return nil
}

func (s *Server) Stop() {
	s.t.Kill(nil)
	s.t.Wait()
}

func errReply(err error) bson.D {
	if err != nil {
		return bson.D{
			{"errmsg", err.Error()},
			{"ok", 0},
		}
	}
	return markOk(nil)
}

func markOk(msg bson.D) bson.D {
	return append(msg, bson.DocElem{"ok", 1})
}

func respDoc(w io.Writer, requestID int32, docs ...interface{}) error {
	resp := NewOpReplyMsg(requestID, docs...)
	return resp.Write(w)
}

func respError(w io.Writer, requestID int32, err error) error {
	if err != nil {
		log.Info(err)
	}
	resp := NewOpReplyMsg(requestID, errReply(err))
	return resp.Write(w)
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	for {
		log.Info("server handle")
		select {
		case <-s.t.Dying():
			return
		default:
		}

		h := &Header{}
		err := h.Read(c)
		if err != nil {
			log.Errorf("header read: %v", err)
			return
		}
		switch h.OpCode {
		case OpInsert:
			log.Info("OpInsert")
			insert, err := NewOpInsertMsg(h)
			if err != nil {
				respError(c, h.RequestID, err)
				return
			}
			s.Backend.HandleInsert(c, insert)
		case OpQuery:
			log.Info("OpQuery")
			query, err := NewOpQueryMsg(h)
			if err != nil {
				respError(c, h.RequestID, err)
				return
			}
			s.Backend.HandleQuery(c, query)
		case OpDelete:
			log.Info("OpDelete")
			deleteMsg, err := NewOpDeleteMsg(h)
			if err != nil {
				respError(c, h.RequestID, err)
				return
			}
			s.Backend.HandleDelete(c, deleteMsg)
		case OpUpdate:
			log.Info("OpUpdate")
			update, err := NewOpUpdateMsg(h)
			if err != nil {
				respError(c, h.RequestID, err)
				return
			}
			s.Backend.HandleUpdate(c, update)
			// TODO: support following options: OpReply, OpMsg, OpGetMore, OpKillCursors
		default:
			err := fmt.Errorf("unsupported op code %d", h.OpCode)
			respError(c, h.RequestID, err)
			return
		}
	}
}
