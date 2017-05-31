package s3tests

s3, err := session.NewSession(
	&aws.Config{Region:aws.String("us-west-2"),
	Credentials: credentials.NewStaticCredentials("AKID", "SECRET_KEY", "TOKEN˓ → "),

})

sess, err := session.NewSessionWithOptions(session.Options{