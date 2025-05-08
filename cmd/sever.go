package main

import (
	"github.com/BIST10000/lazy-forum/pkg/forum"
	"github.com/BIST10000/ws/settings"
)

func main() {
	settings.MiddleHub = settings.NewHub(forum.ForumConnect)
	go settings.MiddleHub.Run()
	for {
	}
}
