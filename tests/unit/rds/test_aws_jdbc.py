from edfred.oob.rds import AWSJdbc


def test_aws_jdbc():
    jdbc_link = "jdbc:postgresql://edfred-edfre-solarcuration-dbadm-euwest1-instance-01.cavpiws4jltu.eu-west-1.rds.amazonaws.com:5432/adm"
    jdbc = AWSJdbc(jdbc_link)
    assert jdbc.awsurl == jdbc_link
    assert jdbc.engine == "postgresql"
    assert jdbc.identifier == "edfred-edfre-solarcuration-dbadm-euwest1-instance-01"
    assert jdbc.account_adress == "cavpiws4jltu"
    assert jdbc.aws_service == "rds"
    assert jdbc.region == "eu-west-1"
    assert jdbc.port == "5432"
    assert jdbc.database == "adm"
