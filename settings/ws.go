package settings

import (
	"github.com/BIST10000/lazy-forum/pkg/forum"
	"github.com/BIST10000/lazy-forum/pkg/log"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
)

var Upgrader = &websocket.Upgrader{
	ReadBufferSize:  0,
	WriteBufferSize: 0,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var MiddleHub *Hub

type Client struct {
	Conn         *websocket.Conn
	Send         chan forum.Message
	DiscussionID int
}

type Hub struct {
	Clients    map[*Client]bool
	Chat       chan forum.Message
	Register   chan *Client
	Unregister chan *Client
	Mutex      sync.Mutex
	Conn       forum.Connect
}

func NewHub(connect forum.Connect) *Hub {
	return &Hub{
		Clients:    make(map[*Client]bool),
		Chat:       make(chan forum.Message),
		Register:   make(chan *Client),
		Mutex:      sync.Mutex{},
		Unregister: make(chan *Client),
		Conn: forum.Connect{
			MsgCase: connect.MsgCase,
			DisCase: connect.DisCase,
		},
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.Register:
			h.Mutex.Lock()
			_, ok := h.Clients[client]
			if ok {
				log.Send.Warn("Пользователь уже есть в чате")
			} else {
				h.Clients[client] = true
			}
			h.Mutex.Unlock()

		case client := <-h.Unregister:
			h.Mutex.Lock()
			_, ok := h.Clients[client]
			if !ok {
				log.Send.Warn("Пользователь нет в чате")
			} else {
				client.Conn.Close()
				delete(h.Clients, client)
			}
			h.Mutex.Unlock()

		case msg := <-h.Chat:
			h.Mutex.Lock()
			for client := range h.Clients {
				if client.DiscussionID == msg.DiscussionID {
					select {
					case client.Send <- msg:
					default:
						client.Conn.Close()
						close(client.Send)
						delete(h.Clients, client)
					}
				}
			}
			h.Mutex.Unlock()
		}
	}
}
