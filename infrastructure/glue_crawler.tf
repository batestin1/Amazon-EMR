resource "aws_glue_catalog_database" "casvstreaming" {
  name = "streamcsvfiles"
}

resource "aws_glue_crawler" "casvstreaming" {
  database_name = aws_glue_catalog_database.casvstreaming.name
  name          = "firehose_stream_s3_crawler"
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.stream.id}/firehose/"
  }

  configuration = <<EOF
{
   "Version": 1.0,
   "Grouping": {
      "TableGroupingPolicy": "CombineCompatibleSchemas" }
}
EOF
}