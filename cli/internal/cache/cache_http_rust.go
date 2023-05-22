//go:build rust
// +build rust

package cache

import (
	client2 "github.com/vercel/turbo/cli/internal/client"
	"github.com/vercel/turbo/cli/internal/ffi"
	"github.com/vercel/turbo/cli/internal/turbopath"
)

func (cache *HttpCache) retrieve(hash string) (bool, []turbopath.AnchoredSystemPath, int, error) {
	apiClient := cache.GetAPIClient().(*client2.APIClient)
	return ffi.HTTPCacheRetrieve(hash, apiClient.GetBaseURL(), apiClient.GetTimeout(), apiClient.GetVersion(), apiClient.GetToken(), apiClient.GetTeamID(), apiClient.GetTeamSlug(), apiClient.GetUsePreflight(), cache.GetAuthenticator().isEnabled(), cache.repoRoot)
}
