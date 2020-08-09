[user@instance-1 ~]$ hbase shell

#create '<table_name>', '<column_family_name>'
hbase(main):002:0> create 'user_profile', 'cf'

#List tablee
hbase(main):003:0> list

#List specific table
hbase(main):004:0> list 'user_profile'

#Insert sample records to the HBase table
#Go thru program than manual

#Display the Content of HBase Table
hbase(main):005:0> scan 'user_profile'
