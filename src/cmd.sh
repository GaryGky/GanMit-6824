build:
  go build -race -buildmode=plugin ../mrapps/wc.go # everytime update mr/ dir should execute this command

clean:
  # run the test in a fresh sub-directory.
  rm -rf mr-tmp
  mkdir mr-tmp || exit 1
  cd mr-tmp || exit 1
  rm -f mr-*

pretty-log:
   ../dslog.py .run/TestFailNoAgree2B_0.log -c 5 -j ERRO,CLNT,LEAD,TEST,LOG1,INFO,LOG2
   ../dslog.py /debug.log -c 3 -j ERRO,CLNT,LEAD,TEST,LOG1,INFO,LOG2

concurrent-test:
  # shellcheck disable=SC2016
  rg 'func (Test.*2B)\(' -oNr '$1' test_test.go | xargs ../dstest.py --workers 20 --iter 50 --output .run --race
  rm -r ./raft/.run
  # shellcheck disable=SC2016
  ../dstest.py TestConcurrentStarts2B --workers 10 --output .run --race --iter 10
