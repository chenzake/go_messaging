package messaging

//{u'oslo.message': u'{"_unique_id": "e7624e57fbc94d45badf00dcede7c8c9", "_msg_id": "33f394e6c17b46b8be88d15e67d834e2", "_timeout": null,
// "_reply_q": "reply_c2f8347976fc4deba414dbe86e089fdb", "args": {"status": "tset"}, "method": "test"}', u'oslo.version': u'2.0'}
import (
	"encoding/json"
	"fmt"
)

type Message struct {
	Msg     interface{}
	Version interface{}
}

func (Msg *Message) Serialize_msg(msg *Message) string {
	j, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("生成json字符串错误")
	}
	fmt.Println(string(j))
	return string(j)
}
