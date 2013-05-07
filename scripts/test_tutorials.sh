sbt assembly
scripts/scald.rb --local tutorial/Tutorial0.scala
scripts/scald.rb --local tutorial/Tutorial1.scala
scripts/scald.rb --local tutorial/Tutorial2.scala
scripts/scald.rb \
  --local tutorial/Tutorial3.scala \
  --input tutorial/data/hello.txt
scripts/scald.rb \
  --local tutorial/Tutorial4.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output4.txt
scripts/scald.rb \
  --local tutorial/Tutorial5.scala \
  --input tutorial/data/hello.txt \
  --output tutorial/data/output5.txt
