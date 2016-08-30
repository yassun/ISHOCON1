worker_processes 9
preload_app true
pid './unicorn.pid'
listen "/tmp/unicorn.sock"
timeout 60000
