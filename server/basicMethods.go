package main

import (
	"errors"
	"fmt"
	"github.com/jeffjenkins/mq/amqp"
	"time"
)

func (channel *Channel) basicRoute(methodFrame amqp.MethodFrame) error {
	switch method := methodFrame.(type) {
	case *amqp.BasicQos:
		return channel.basicQos(method)
	case *amqp.BasicRecover:
		return channel.basicRecover(method)
	case *amqp.BasicNack:
		return channel.basicNack(method)
	case *amqp.BasicConsume:
		return channel.basicConsume(method)
	case *amqp.BasicCancel:
		return channel.basicCancel(method)
	case *amqp.BasicCancelOk:
		return channel.basicCancelOk(method)
	case *amqp.BasicPublish:
		return channel.basicPublish(method)
	case *amqp.BasicGet:
		return channel.basicGet(method)
	case *amqp.BasicAck:
		return channel.basicAck(method)
	case *amqp.BasicReject:
		return channel.basicReject(method)
	}
	return errors.New("Unable to route method frame")
}

func (channel *Channel) basicQos(method *amqp.BasicQos) error {
	if method.Global {
		channel.prefetchSize = method.PrefetchSize
		channel.prefetchCount = method.PrefetchCount
	} else {
		channel.defaultPrefetchSize = method.PrefetchSize
		channel.defaultPrefetchCount = method.PrefetchCount
	}
	channel.sendMethod(&amqp.BasicQosOk{})
	return nil
}

func (channel *Channel) basicRecover(method *amqp.BasicRecover) error {
	var classId, methodId = method.MethodIdentifier()
	channel.conn.connectionErrorWithMethod(540, "Not implemented", classId, methodId)

	fmt.Println("Handling BasicRecover")
	return nil
}

func (channel *Channel) basicNack(method *amqp.BasicNack) error {
	var classId, methodId = method.MethodIdentifier()
	channel.conn.connectionErrorWithMethod(540, "Not implemented", classId, methodId)

	fmt.Println("Handling BasicNack")
	return nil
}

func (channel *Channel) basicConsume(method *amqp.BasicConsume) error {
	fmt.Println("Handling BasicConsume")
	var queue, found = channel.conn.server.queues[method.Queue]
	if !found {
		// TODO(MUST): not found error? spec xml doesn't say
		return errors.New("Queue not found")
	}
	if len(method.ConsumerTag) == 0 {
		method.ConsumerTag = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	queue.addConsumer(channel, method)
	if !method.NoWait {
		channel.sendMethod(&amqp.BasicConsumeOk{method.ConsumerTag})
	}

	return nil
}

func (channel *Channel) basicCancel(method *amqp.BasicCancel) error {

	if err := channel.removeConsumer(method.ConsumerTag); err != nil {
		var classId, methodId = method.MethodIdentifier()
		channel.channelErrorWithMethod(404, "Consumer not found", classId, methodId)
		return nil
	}

	fmt.Println("Handling BasicCancel")
	if !method.NoWait {
		channel.sendMethod(&amqp.BasicCancelOk{method.ConsumerTag})
	}
	return nil
}

func (channel *Channel) basicCancelOk(method *amqp.BasicCancelOk) error {
	// TODO(MAY)
	var classId, methodId = method.MethodIdentifier()
	channel.conn.connectionErrorWithMethod(540, "Not implemented", classId, methodId)

	fmt.Println("Handling BasicCancelOk")
	return nil
}

func (channel *Channel) basicPublish(method *amqp.BasicPublish) error {
	channel.lastMethodFrame = method
	return nil
}

func (channel *Channel) basicGet(method *amqp.BasicGet) error {
	fmt.Println("Handling BasicGet")
	var classId, methodId = method.MethodIdentifier()
	channel.conn.connectionErrorWithMethod(540, "Not implemented", classId, methodId)
	return nil
}

func (channel *Channel) basicAck(method *amqp.BasicAck) error {
	fmt.Println("Handling BasicAck")
	var ok = false
	if method.Multiple {
		ok = channel.ackBelow(method.DeliveryTag)
	} else {
		ok = channel.ackOne(method.DeliveryTag)
	}
	if !ok {
		var classId, methodId = method.MethodIdentifier()
		var msg = "Precondition Failed: Delivery Tag not found"
		channel.channelErrorWithMethod(406, msg, classId, methodId)
	}
	return nil
}

func (channel *Channel) basicReject(method *amqp.BasicReject) error {
	fmt.Printf("Handling BasicReject: %d\n", method.DeliveryTag)
	if err := channel.rejectMessage(method.DeliveryTag); err != nil {
		var classId, methodId = method.MethodIdentifier()
		channel.channelErrorWithMethod(404, "Consumer not found", classId, methodId)
		return nil
	}
	return nil
}
