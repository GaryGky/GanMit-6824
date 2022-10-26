build:
  go build -race -buildmode=plugin ../mrapps/wc.go # everytime update mr/ dir should execute this command

clean:
  # run the test in a fresh sub-directory.
  rm -rf mr-tmp
  mkdir mr-tmp || exit 1
  cd mr-tmp || exit 1
  rm -f mr-*

pretty-log:
   ./script/dslog.py ./raft/debug.log -j ERRO,CLNT,LEAD,TEST,LOG1 -c 3