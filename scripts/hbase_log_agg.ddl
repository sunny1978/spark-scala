[user@instance-1 ~]$ hbase shell

#create '<table_name>', '<column_family_name>'
hbase(main):002:0> create 'log_agg', 'cf'

#List tablee
hbase(main):003:0> list

#List specific table
hbase(main):004:0> list 'log_agg'

#Insert sample records to the HBase table
#Go thru program than manual

#Display the Content of HBase Table
hbase(main):005:0> scan 'log_agg'
