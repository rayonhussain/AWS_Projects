--this is when we give colname=value as a folder name in s3, it will auto partition
--MSCK REPAIR TABLE `iris_hive_partition`;

--this is manual PARTITION and hard coding
--ALTER TABLE iris_partition ADD PARTITION (partition_0='versicolor') location 's3://iris-rayon/partition/versicolor/';

select * from iris_hive_partition