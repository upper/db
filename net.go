// Copyright (c) 2012-2015 The upper.io/db authors. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package db

import (
	"fmt"
	"strconv"
	"strings"
)

// Address is an interface that represents the host part of an URL.
type Address interface {
	String() string
	Host() (string, error)
	Port() (uint, error)
	Path() (string, error)
}

// SocketAddr is a UNIX address.
type SocketAddr struct {
	path string
}

// HostAddr is the name or IP of a server coupled with an optional port number.
type HostAddr struct {
	name string
	port uint
}

// ParseAddress parses s into a host or socket structures.
func ParseAddress(s string) Address {
	if strings.HasPrefix(s, "/") {
		// Let's suppose this is a UNIX socket.
		return SocketAddr{path: s}
	}
	hp := strings.Split(s, ":")
	if len(hp) > 1 {
		p, _ := strconv.Atoi(hp[1])
		return HostAddr{name: hp[0], port: uint(p)}
	}
	return HostAddr{name: hp[0]}
}

// Host converts the given name into a host structure.
func Host(name string) HostAddr {
	return HostAddr{name: name}
}

// Socket converts the given name into a socket structure.
func Socket(path string) SocketAddr {
	return SocketAddr{path}
}

// HostPort converts the given name and port into a host structure.
func HostPort(name string, port uint) HostAddr {
	if port > 0 {
		return HostAddr{name: name, port: port}
	}
	return HostAddr{name: name}
}

// String returns the string representation of the host struct.
func (h HostAddr) String() string {
	if h.port > 0 {
		return fmt.Sprintf("%s:%d", h.name, h.port)
	}
	return h.name
}

// Host returns the hostname/ip part of the host struct.
func (h HostAddr) Host() (string, error) {
	if h.name != "" {
		return h.name, nil
	}
	return "", ErrUndefined
}

// Port returns the port number of the host struct.
func (h HostAddr) Port() (uint, error) {
	if h.port > 0 {
		return h.port, nil
	}
	return 0, ErrUndefined
}

// Path is undefined in a host struct.
func (h HostAddr) Path() (string, error) {
	return "", ErrUndefined
}

// String() returns the string representation of the socket struct.
func (s SocketAddr) String() string {
	return s.path
}

// Host is undefined in a socket struct.
func (s SocketAddr) Host() (string, error) {
	return "", ErrUndefined
}

// Port is undefined in a socket struct.
func (s SocketAddr) Port() (uint, error) {
	return 0, ErrUndefined
}

// Path returns the file path of the socket struct.
func (s SocketAddr) Path() (string, error) {
	return s.path, nil
}
