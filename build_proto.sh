PROTO_DIR=internal/transport/grpc/
for proto in $(find $PROTO_DIR -name "*.proto")
    do
        bname=`basename $proto .proto`
        mkdir -p $PROTO_DIR/$bname
        echo "Compiling $bname.proto ..."
        protoc --proto_path=$PWD/$PROTO_DIR --go_out=./$PROTO_DIR/$(basename $proto .proto) --go-grpc_out=./$PROTO_DIR/$(basename $proto .proto) --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative $PWD/$proto
        echo "done."
    done