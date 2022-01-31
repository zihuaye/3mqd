package server

import (
	"github.com/jeffjenkins/dispatchd/amqp"
	"github.com/jeffjenkins/dispatchd/stats"
	"github.com/jeffjenkins/dispatchd/util"
)

func (channel *Channel) basicRoute(methodFrame amqp.MethodFrame) *amqp.AMQPError {
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
	var classId, methodId = methodFrame.MethodIdentifier()
	return amqp.NewHardError(540, "Unable to route method frame", classId, methodId)
}

func (channel *Channel) basicQos(method *amqp.BasicQos) *amqp.AMQPError {
	channel.setPrefetch(method.PrefetchCount, method.PrefetchSize, method.Global)
	channel.SendMethod(&amqp.BasicQosOk{})
	return nil
}

func (channel *Channel) basicRecover(method *amqp.BasicRecover) *amqp.AMQPError {
	channel.recover(method.Requeue)
	channel.SendMethod(&amqp.BasicRecoverOk{})
	return nil
}

func (channel *Channel) basicNack(method *amqp.BasicNack) *amqp.AMQPError {
	if method.Multiple {
		return channel.nackBelow(method.DeliveryTag, method.Requeue, false)
	}
	return channel.nackOne(method.DeliveryTag, method.Requeue, false)
}

func (channel *Channel) basicConsume(method *amqp.BasicConsume) *amqp.AMQPError {
	var classId, methodId = method.MethodIdentifier()
	// Check queue
	if len(method.Queue) == 0 {
		if len(channel.lastQueueName) == 0 {
			return amqp.NewSoftError(404, "Queue not found", classId, methodId)
		} else {
			method.Queue = channel.lastQueueName
		}
	}
	// TODO: do not directly access channel.conn.server.queues
	var queue, found = channel.conn.server.queues[method.Queue]
	if !found {
		// Spec doesn't say, but seems like a 404?
		return amqp.NewSoftError(404, "Queue not found", classId, methodId)
	}
	if len(method.ConsumerTag) == 0 {
		method.ConsumerTag = util.RandomId()
	}
	amqpErr := channel.addConsumer(queue, method)
	if amqpErr != nil {
		return amqpErr
	}
	if !method.NoWait {
		channel.SendMethod(&amqp.BasicConsumeOk{ConsumerTag: method.ConsumerTag})
	}

	return nil
}

func (channel *Channel) basicCancel(method *amqp.BasicCancel) *amqp.AMQPError {

	if err := channel.removeConsumer(method.ConsumerTag); err != nil {
		var classId, methodId = method.MethodIdentifier()
		return amqp.NewSoftError(404, "Consumer not found", classId, methodId)
	}

	if !method.NoWait {
		channel.SendMethod(&amqp.BasicCancelOk{ConsumerTag: method.ConsumerTag})
	}
	return nil
}

func (channel *Channel) basicCancelOk(method *amqp.BasicCancelOk) *amqp.AMQPError {
	// TODO(MAY)
	var classId, methodId = method.MethodIdentifier()
	return amqp.NewHardError(540, "Not implemented", classId, methodId)
}

func (channel *Channel) basicPublish(method *amqp.BasicPublish) *amqp.AMQPError {
	defer stats.RecordHisto(channel.statPublish, stats.Start())
	var _, found = channel.server.exchanges[method.Exchange]
	if !found {
		var classId, methodId = method.MethodIdentifier()
		return amqp.NewSoftError(404, "Exchange not found", classId, methodId)
	}
	channel.startPublish(method)
	return nil
}

func (channel *Channel) basicGet(method *amqp.BasicGet) *amqp.AMQPError {
	// var classId, methodId = method.MethodIdentifier()
	// channel.conn.connectionErrorWithMethod(540, "Not implemented", classId, methodId)
	var queue, found = channel.conn.server.queues[method.Queue]
	if !found {
		// Spec doesn't say, but seems like a 404?
		var classId, methodId = method.MethodIdentifier()
		return amqp.NewSoftError(404, "Queue not found", classId, methodId)
	}
	var qm = queue.GetOneForced()
	if qm == nil {
		channel.SendMethod(&amqp.BasicGetEmpty{})
		return nil
	}

	var rhs = []amqp.MessageResourceHolder{channel}
	msg, err := channel.server.msgStore.GetAndDecrRef(qm, queue.Name, rhs)
	if err != nil {
		// TODO: return 500 error
		channel.SendMethod(&amqp.BasicGetEmpty{})
		return nil
	}

	channel.SendContent(&amqp.BasicGetOk{
		DeliveryTag:  channel.nextDeliveryTag(),
		Redelivered:  qm.DeliveryCount > 0,
		Exchange:     msg.Exchange,
		RoutingKey:   msg.Key,
		MessageCount: 1,
	}, msg)
	return nil
}

func (channel *Channel) basicAck(method *amqp.BasicAck) *amqp.AMQPError {
	if method.Multiple {
		return channel.ackBelow(method.DeliveryTag, false)
	}
	return channel.ackOne(method.DeliveryTag, false)
}

func (channel *Channel) basicReject(method *amqp.BasicReject) *amqp.AMQPError {
	return channel.nackOne(method.DeliveryTag, method.Requeue, false)
}
