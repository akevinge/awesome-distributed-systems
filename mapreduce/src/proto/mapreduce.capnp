@0xfc75fd197c57d8f3;

using Rust = import "rust.capnp";

$Rust.parentModule("proto");

struct KeyValue {
  key @0 :Text;
  value @1 :Text;
}

struct Intermediate {
  entries @0 :List(KeyValue);
}


struct MapTask {
  id @0 :UInt64;
  file @1 :Text;
  partitions @2 :UInt32;
}

struct ReduceTask {
  id @0 :UInt64;
  intermediates @1 :List(Text);
}

struct Task {
  union {
    map @0 :MapTask;
    reduce @1 :ReduceTask;
  }
}

interface Coordinator {
  healthCheck @0 () -> ();

  struct AssignTaskResponse {
    task @0 :Task;
  }

  assignTask @1 () -> (response :AssignTaskResponse);

  struct FinishedTaskRequest {
    struct IntermediateLocationWithReduceId {
      reduceId @0 :UInt64;
      intermediate @1 :Text;
    }

    id @0 :UInt64;

    union {
      map @1 :List(IntermediateLocationWithReduceId);
      reduce @2 :Void;
    }
  }

  finishedTask @2 (request: FinishedTaskRequest) -> ();
}

