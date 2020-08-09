#Yarn Process Kill
yarn app -list
yarn app -kill $1

#Driver process kill
ps -aef | grep spark-streaming_2.11-0.1.jar
kill -9 $2

