// Forwarding header — resolves `#include "common/types.h"` from
// `raft/raft_node.h` (and other raft/ headers) to the actual my-etcd
// `common/types.h` even when the project's own `common/types.h` shadows
// it in the `-I` search path.
#pragma once
#include "../../common/types.h"
