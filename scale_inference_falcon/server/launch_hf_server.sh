model=tiiuae/falcon-7b-instruct
num_shard=1
volume=$PWD/data 
docker run --gpus all --shm-size 1g -p 8080:80 -v $volume:/data ghcr.io/huggingface/text-generation-inference:0.8 --model-id $model --num-shard $num_shard