<h1 align="center">Web Parts</h1>

## Features

This is an assorted set of Go packages shared between a couple of Inchworks web applications.

- limiterhandler : rate limiting of HTTP requests to mitigate password guessing and other probes.
- monitor : maintains and reports the liveness of a set of clients that are polling a server.
- multiforms : handling of HTTP forms with sub-forms.
- server : an HTTPS web server with an idiosyncratic configuration.
- stack : combines files and templates from packages, app, and site customisation.
- uploader : management of uploaded images and videos.
- users : user signup, log-in and management for a pre-approved set of application users.

For examples of use, see https://github.com/inchworks/picinch.

## Contributing

This is work in progress, and likely to change.
So I can only accept pull requests for minor fixes or improvements to existing facilities.
Please open issues to discuss new features.

## Acknowledgments

Go Packages
- [disintegration/imaging](https://github.com/disintegration/imaging) Image processing.
- [golangcollege/sessions](https://github.com/golangcollege/sessions) HTTP session cookies.