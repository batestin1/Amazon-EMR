resource "aws_s3_bucket_object" "assassinatos" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/assassinatos.csv"
  acl    = "private"
  source = "../data/assassinatos.csv"
  etag   = filemd5("../data/assassinatos.csv")
}

resource "aws_s3_bucket_object" "casamento" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/casamento.csv"
  acl    = "private"
  source = "../data/casamento.csv"
  etag   = filemd5("../data/casamento.csv")
}

resource "aws_s3_bucket_object" "dc" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/dc.csv"
  acl    = "private"
  source = "../data/dc.csv"
  etag   = filemd5("../data/dc.csv")
}

resource "aws_s3_bucket_object" "fifa" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/fifa.csv"
  acl    = "private"
  source = "../data/fifa.csv"
  etag   = filemd5("../data/fifa.csv")
}

resource "aws_s3_bucket_object" "filmes" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/filmes.csv"
  acl    = "private"
  source = "../data/filmes.csv"
  etag   = filemd5("../data/filmes.csv")
}

resource "aws_s3_bucket_object" "harrypotter" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/harrypotter.csv"
  acl    = "private"
  source = "../data/harrypotter.csv"
  etag   = filemd5("../data/harrypotter.csv")
}

resource "aws_s3_bucket_object" "marvel" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/marvel.csv"
  acl    = "private"
  source = "../data/marvel.csv"
  etag   = filemd5("../data/marvel.csv")
}

resource "aws_s3_bucket_object" "nba" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/nba.csv"
  acl    = "private"
  source = "../data/nba.csv"
  etag   = filemd5("../data/nba.csv")
}

resource "aws_s3_bucket_object" "olimpiadas" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/olimpiadas.csv"
  acl    = "private"
  source = "../data/olimpiadas.csv"
  etag   = filemd5("../data/olimpiadas.csv")
}

resource "aws_s3_bucket_object" "senhordosaneis" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/senhordosaneis.csv"
  acl    = "private"
  source = "../data/senhordosaneis.csv"
  etag   = filemd5("../data/senhordosaneis.csv")
}

resource "aws_s3_bucket_object" "tarantino" {
  bucket = aws_s3_bucket.dlt.id
  key    = "data/tarantino.csv"
  acl    = "private"
  source = "../data/tarantino.csv"
  etag   = filemd5("../data/tarantino.csv")
}

resource "aws_s3_bucket_object" "insert" {
  bucket = aws_s3_bucket.dla.id
  key    = "pyspark/insert.py"
  acl    = "private"
  source = "../scripts/pyspark/insert.py"
  etag   = filemd5("../scripts/pyspark/insert.py")
}

resource "aws_s3_bucket_object" "upsert" {
  bucket = aws_s3_bucket.dla.id
  key    = "pyspark/upsert.py"
  acl    = "private"
  source = "../scripts/pyspark/upsert.py"
  etag   = filemd5("../scripts/pyspark/upsert.py")
}
