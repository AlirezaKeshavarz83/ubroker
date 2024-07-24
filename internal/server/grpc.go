package server

import (
	"context"
	"fmt"
	"ubroker/pkg/ubroker"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcServicer struct {
	broker ubroker.Broker
}

func NewGRPC(broker ubroker.Broker) ubroker.BrokerServer {
	return &grpcServicer{
		broker: broker,
	}
}

func (s *grpcServicer) Fetch(stream ubroker.Broker_FetchServer) error {

	ctx := stream.Context()

	ch, err := s.broker.Delivery(ctx)

	if err != nil {
		return convertError(err)
	}

	for {
		_, err := stream.Recv()
		if err != nil {
			return nil
		}
		stream.Send(<-ch)
	}

	return status.Error(codes.Unimplemented, "not implemented")
}

func (s *grpcServicer) Acknowledge(ctx context.Context, request *ubroker.AcknowledgeRequest) (*empty.Empty, error) {
	return &empty.Empty{}, convertError(s.broker.Acknowledge(ctx, request.Id))
}

func (s *grpcServicer) ReQueue(ctx context.Context, request *ubroker.ReQueueRequest) (*empty.Empty, error) {
	return &empty.Empty{}, convertError(s.broker.ReQueue(ctx, request.Id))
}

func (s *grpcServicer) Publish(ctx context.Context, request *ubroker.Message) (*empty.Empty, error) {
	fmt.Println("this?")
	return &empty.Empty{}, convertError(s.broker.Publish(ctx, request))
}

func convertError(err error) error {
	switch err {
	case ubroker.ErrClosed:
		return status.Error(codes.Unavailable, "Unavailable")
	}
	return nil
}
