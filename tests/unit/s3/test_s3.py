from edfred.oob.s3 import S3Bucket, S3Object
from boto3 import client
from moto import mock_s3


@mock_s3
def test_bucket():
    s3 = client("s3", region_name="eu-west-1")
    s3.create_bucket(Bucket="my-bucket")

    bucket_manager = S3Bucket(name="my-bucket")
    print(bucket_manager.arn)
    assert bucket_manager.name == "my-bucket"

    bucket_manager = S3Bucket(arn="arn:aws:s3:::my-bucket")
    assert bucket_manager.name == "my-bucket"


@mock_s3
def test_s3(tmpdir):
    # TODO: use mock files
    s3 = client("s3", region_name="eu-west-1")
    s3.create_bucket(Bucket="my-bucket")
    bucket_manager = S3Bucket(name="my-bucket")
    p = tmpdir.join("local.txt")
    p.write("test")
    bucket_manager.upload_file(str(p), "distant.txt")
    obj = bucket_manager.get_object("distant.txt")
    p2 = tmpdir.join("distant.txt")
    obj.download_to(str(p2))
    with p2.open("r") as f:
        assert f.read() == "test"
    with obj.download_fileobj() as f:
        assert f.read() == b"test"

    obj_copy = obj.copy_to(bucket_manager.name, "folder/copy.txt")

    assert obj_copy.attributes.content_length == 4

    assert bucket_manager.list_keys() == ["distant.txt", "folder/copy.txt"]

    obj.delete()

    assert bucket_manager.list_keys() == ["folder/copy.txt"]

    bucket_manager.delete_directory("folder")

    assert bucket_manager.list_keys() == []
