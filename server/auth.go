package server

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	//"fmt"
	"golang.org/x/crypto/bcrypt"
)

type User struct {
	name     string
	password []byte
}

func (s *Server) addUsers(userJson map[string]interface{}) {
	if len(userJson) == 0 {
		decoded, err := base64.StdEncoding.DecodeString(defaultUserPasswordBase64)
		if err != nil {
			panic("System integrity error: Could not base64 decode password for built in default user!")
		}
		s.users[defaultUserName] = User{name: defaultUserName, password: decoded}
	}
	for name, user := range userJson {
		var encoded = user.(map[string]interface{})["password_bcrypt_base64"].(string)
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			panic("Could not base64 decode password for default config file user: " + name)
		}
		s.users[name] = User{name: name, password: decoded}
	}
}

// guest/guest
var defaultUserName = "guest"
var defaultUserPasswordBase64 = "JDJhJDExJENobGk4dG5rY0RGemJhTjhsV21xR3VNNnFZZ1ZqTzUzQWxtbGtyMHRYN3RkUHMuYjF5SUt5"

func (s *Server) authenticate(mechanism string, blob []byte) bool {
	var username string
	var passwd []byte

	if mechanism == "PLAIN" {
		// Split. SASL PLAIN has three parts
		parts := bytes.Split(blob, []byte{0})
		if len(parts) != 3 {
			return false
		}

		username = string(parts[1])
		passwd = parts[2]
	} else if mechanism == "AMQPLAIN" {
		s1 := bytes.Index(blob, []byte("LOGIN")) + 6
		s2 := bytes.Index(blob, []byte("PASSWORD")) + 9

		i1 := binary.BigEndian.Uint32(blob[s1 : s1+4])
		i2 := binary.BigEndian.Uint32(blob[s2 : s2+4])

		//fmt.Printf("s1:%d s2:%d i1:%d i2:%d\n", s1, s2, i1, i2)

		username = string(blob[s1+4 : s1+4+int(i1)])
		passwd = blob[s2+4 : s2+4+int(i2)]
		//fmt.Printf("username:%s, password:%s", username, string(passwd))
	}

	for name, user := range s.users {
		if username != name {
			continue
		}
		err := bcrypt.CompareHashAndPassword(user.password, passwd)
		if err == nil {
			return true
		}
	}
	return false
}
