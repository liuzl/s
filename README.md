# S

[![GoDoc](https://godoc.org/github.com/liuzl/s?status.svg)](https://godoc.org/github.com/liuzl/s)

S is a simple persistent message/task stack. It is written in Golang.

It has the following features:

- [Message confirmation and retry](#confirm-retry)
- [Message persistent](#persistent)

Messages in S are very safe. They exist until the consumer confirms clearly, otherwise messages would be recycled and added back to stack again.
