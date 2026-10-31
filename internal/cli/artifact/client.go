package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/smellak/gifford/internal/endpoint"
	hubapi "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/xdg"
)

type artifactClient struct {
	hub   *endpoint.SocketHub
	agent protocol.AgentID
	dirs  xdg.Dirs
}

func openArtifactClient(ctx context.Context) (*artifactClient, error) {
	dirs, err := xdg.Resolve()
	if err != nil {
		return nil, fmt.Errorf("resolve artifact directories: %w", err)
	}
	client, err := endpoint.DialHub(ctx, filepath.Join(dirs.Runtime, "hub.sock"))
	if err != nil {
		return nil, fmt.Errorf("artifact hub: %w", err)
	}
	agent, err := client.WhoAmI(ctx)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("artifact identity: %w", err)
	}
	answer := &artifactClient{hub: client, agent: agent, dirs: dirs}
	if err := answer.publishIdentity(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return answer, nil
}

func (c *artifactClient) close() { _ = c.hub.Close() }

func (c *artifactClient) publishIdentity(ctx context.Context) error {
	signing, err := endpoint.EnsureAgentKey(c.dirs.Data, c.agent)
	if err != nil {
		return fmt.Errorf("ensure artifact signing key: %w", err)
	}
	encryption, err := endpoint.EnsureAgentEncryptionKey(c.dirs.Data, c.agent)
	if err != nil {
		return fmt.Errorf("ensure artifact encryption key: %w", err)
	}
	process, err := registry.CurrentProcess()
	if err != nil {
		return fmt.Errorf("describe artifact process: %w", err)
	}
	raw, err := c.hub.Call(ctx, hubapi.MethodRegisterEndpoint, hubapi.RegisterEndpointParams{
		Process: process, PublicKey: signing,
		KeyID:               "ed25519:" + string(c.agent) + ":v1",
		EncryptionPublicKey: encryption,
		EncryptionKeyID:     "x25519:" + string(c.agent) + ":v1",
		PublishOnly:         true,
	})
	if err != nil {
		return artifactHubError("register artifact identity", err)
	}
	var result hubapi.RegisterEndpointResult
	if err := json.Unmarshal(raw, &result); err != nil || !result.Active || !result.EncryptionActive ||
		result.KeyID != "ed25519:"+string(c.agent)+":v1" ||
		result.EncryptionKeyID != "x25519:"+string(c.agent)+":v1" {
		return fmt.Errorf("artifact identity is not active")
	}
	return nil
}

func (c *artifactClient) call(ctx context.Context, method string, params, result any) error {
	raw, err := c.hub.Call(ctx, method, params)
	if err != nil {
		return artifactHubError(method, err)
	}
	if err := json.Unmarshal(raw, result); err != nil {
		return fmt.Errorf("decode %s result: %w", method, err)
	}
	if validator, ok := result.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("validate %s result: %w", method, err)
		}
	}
	return nil
}

func artifactHubError(operation string, err error) error {
	var failure *endpoint.HubError
	if errors.As(err, &failure) && failure.ErrorCode.Valid() {
		return fmt.Errorf("%s: %s", operation, failure.ErrorCode)
	}
	return fmt.Errorf("%s: hub unavailable", operation)
}

func listArtifacts(ctx context.Context) (protocol.ListPendingArtifactsResult, error) {
	client, err := openArtifactClient(ctx)
	if err != nil {
		return protocol.ListPendingArtifactsResult{}, err
	}
	defer client.close()
	var result protocol.ListPendingArtifactsResult
	err = client.call(ctx, hubapi.MethodListPendingArtifacts,
		protocol.ListPendingArtifactsParams{}, &result)
	return result, err
}

func artifactStatus(ctx context.Context, id protocol.OpaqueID) (protocol.ArtifactStatusResult, error) {
	if !id.Valid() {
		return protocol.ArtifactStatusResult{}, fmt.Errorf("invalid artifact id")
	}
	client, err := openArtifactClient(ctx)
	if err != nil {
		return protocol.ArtifactStatusResult{}, err
	}
	defer client.close()
	var result protocol.ArtifactStatusResult
	err = client.call(ctx, hubapi.MethodGetArtifactStatus,
		protocol.ArtifactIDParams{ArtifactID: id}, &result)
	return result, err
}
