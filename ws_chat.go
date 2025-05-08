package chat

import (
	"encoding/json"
	"github.com/BIST10000/lazy-forum/pkg/forum"
	"github.com/BIST10000/lazy-forum/pkg/log"
	"github.com/BIST10000/ws/settings"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"strconv"
)

func Chat(c *gin.Context) {
	conn, err := settings.Upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Send.Error("Ошибка апгрейда сети",
			zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка апгрейда сети"})
		return
	}

	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		log.Send.Error("Ошибка парсинга id",
			zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка парсинга id"})
		return
	}

	client := &settings.Client{
		Conn:         conn,
		Send:         make(chan forum.Message, 256),
		DiscussionID: id,
	}

	settings.MiddleHub.Register <- client
	go func() {
		messages, err := settings.MiddleHub.Conn.MsgCase.GetAllMessage(id)
		if err != nil {
			log.Send.Error("Ошибка получения сообщений обсуждения",
				zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Ошибка получения сообщений обсуждения"})
			return
		}

		for _, msg := range messages {
			select {
			case client.Send <- msg:
			default:
				log.Send.Fatal("Буффер пользователя переполнен")
				client.Conn.Close()
				settings.MiddleHub.Unregister <- client
			}
		}

		for {
			_, m, err := conn.ReadMessage()
			if err != nil {
				log.Send.Error("Ошибка чтения сообщения",
					zap.Error(err))
				break
			}

			var msg forum.Message
			if err := json.Unmarshal(m, &msg); err != nil {
				log.Send.Warn("Некорректный формат сообщения",
					zap.Error(err))
				conn.WriteJSON(map[string]string{"error": "Некорректный формат сообщения"})
				continue
			}

			msg.DiscussionID = id
			err = settings.MiddleHub.Conn.MsgCase.CreateMessage(msg)
			if err != nil {
				continue
			}
			settings.MiddleHub.Chat <- msg
		}
	}()

	go func() {
		defer conn.Close()

		for msg := range client.Send {
			m, err := json.Marshal(&msg)
			if err != nil {
				log.Send.Error("Ошибка сериализации сообщения",
					zap.Error(err),
					zap.Any("msgID", msg.DiscussionID))
				continue
			}

			if err := conn.WriteMessage(websocket.TextMessage, m); err != nil {
				log.Send.Error("Ошибка отправки сообщения по вебсокету",
					zap.Error(err),
					zap.Any("msgID", msg.DiscussionID))
				break
			}
		}
	}()
}
