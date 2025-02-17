// Package ftp implements a FTP client as described in RFC 959.
//
// A textproto.Error is returned for errors at the protocol level.
package ftp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// EntryType describes the different types of an Entry.
type EntryType int

// The differents types of an Entry
const (
	EntryTypeFile EntryType = iota
	EntryTypeFolder
	EntryTypeLink
)

// ServerConn represents the connection to a remote FTP server.
// A single connection only supports one in-flight data connection.
// It is not safe to be called concurrently.
type ServerConn struct {
	options *dialOptions
	conn    *textproto.Conn
	host    string

	// Server capabilities discovered at runtime
	features      map[string]string
	skipEPSV      bool
	mlstSupported bool
}

// DialOption represents an option to start a new connection with Dial
type DialOption struct {
	setup func(do *dialOptions)
}

// dialOptions contains all the options set by DialOption.setup
type dialOptions struct {
	dialer      net.Dialer
	tlsConfig   *tls.Config
	conn        net.Conn
	disableEPSV bool
	location    *time.Location
	debugOutput io.Writer
	dialFunc    func(ctx context.Context, network, address string) (net.Conn, error)
	dcTimeout   time.Duration
}

// Entry describes a file and is returned by List().
type Entry struct {
	Name   string
	Target string // target of symbolic link
	Type   EntryType
	Size   uint64
	Time   time.Time
}

// Response represents a data-connection
type Response struct {
	conn   net.Conn
	c      *ServerConn
	closed bool
}

// Responser interface on a data-connection
type Responser interface {
	io.Reader
	io.Closer
	// SetDeadline sets the deadlines associated with the connection.
	SetDeadline(t time.Time) error
}

// Dial connects to the specified address with optional options
func Dial(ctx context.Context, addr string, options ...DialOption) (*ServerConn, error) {
	do := &dialOptions{}
	for _, option := range options {
		option.setup(do)
	}

	if do.location == nil {
		do.location = time.UTC
	}

	tconn := do.conn
	if tconn == nil {
		var err error

		if do.dialFunc != nil {
			tconn, err = do.dialFunc(ctx, "tcp", addr)
		} else if do.tlsConfig != nil {
			tconn, err = tls.DialWithDialer(&do.dialer, "tcp", addr, do.tlsConfig)
		} else {
			tconn, err = do.dialer.DialContext(ctx, "tcp", addr)
		}
		if err != nil {
			return nil, err
		}
	}

	// Use the resolved IP address in case addr contains a domain name
	// If we use the domain name, we might not resolve to the same IP.
	remoteAddr := tconn.RemoteAddr().(*net.TCPAddr)

	var sourceConn io.ReadWriteCloser = tconn
	if do.debugOutput != nil {
		sourceConn = newDebugWrapper(tconn, do.debugOutput)
	}

	c := &ServerConn{
		options:  do,
		features: make(map[string]string),
		conn:     textproto.NewConn(sourceConn),
		host:     remoteAddr.IP.String(),
	}

	_, _, err := c.conn.ReadResponse(StatusReady)
	if err != nil {
		c.Quit()
		return nil, err
	}

	err = c.feat(ctx)
	if err != nil {
		c.Quit()
		return nil, err
	}

	if _, mlstSupported := c.features["MLST"]; mlstSupported {
		c.mlstSupported = true
	}

	return c, nil
}

// DialWithTimeout returns a DialOption that configures the ServerConn with specified timeout
func DialWithTimeout(timeout time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.Timeout = timeout
	}}
}

// DialWithDialer returns a DialOption that configures the ServerConn with specified net.Dialer
func DialWithDialer(dialer net.Dialer) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer = dialer
	}}
}

// DialWithNetConn returns a DialOption that configures the ServerConn with the underlying net.Conn
func DialWithNetConn(conn net.Conn) DialOption {
	return DialOption{func(do *dialOptions) {
		do.conn = conn
	}}
}

// DialWithDisabledEPSV returns a DialOption that configures the ServerConn with EPSV disabled
// Note that EPSV is only used when advertised in the server features.
func DialWithDisabledEPSV(disabled bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.disableEPSV = disabled
	}}
}

// DialWithLocation returns a DialOption that configures the ServerConn with specified time.Location
// The location is used to parse the dates sent by the server which are in server's timezone
func DialWithLocation(location *time.Location) DialOption {
	return DialOption{func(do *dialOptions) {
		do.location = location
	}}
}

// DialWithTLS returns a DialOption that configures the ServerConn with specified TLS config
//
// If called together with the DialWithDialFunc option, the DialWithDialFunc function
// will be used when dialing new connections but regardless of the function,
// the connection will be treated as a TLS connection.
func DialWithTLS(tlsConfig *tls.Config) DialOption {
	return DialOption{func(do *dialOptions) {
		do.tlsConfig = tlsConfig
	}}
}

// DialWithDebugOutput returns a DialOption that configures the ServerConn to write to the Writer
// everything it reads from the server
func DialWithDebugOutput(w io.Writer) DialOption {
	return DialOption{func(do *dialOptions) {
		do.debugOutput = w
	}}
}

// DialWithDialFunc returns a DialOption that configures the ServerConn to use the
// specified function to establish both control and data connections
//
// If used together with the DialWithNetConn option, the DialWithNetConn
// takes precedence for the control connection, while data connections will
// be established using function specified with the DialWithDialFunc option
func DialWithDialFunc(f func(ctx context.Context, network, address string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialFunc = f
	}}
}

// DialWithDataConnectionTimeout returns a DialOption that configures the ServerConn with timeout
// for reading from data connection.
// It only affects data socket which is internally created for commands like LIST.
// It is "dynamic", which means it is restarted after each data portion arrives.
func DialWithDataConnectionTimeout(timeout time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dcTimeout = timeout
	}}
}

// DialTimeout initializes the connection to the specified ftp server address.
//
// It is generally followed by a call to Login() as most FTP commands require
// an authenticated user.
func DialTimeout(ctx context.Context, addr string, timeout time.Duration) (*ServerConn, error) {
	return Dial(ctx, addr, DialWithTimeout(timeout))
}

// Login authenticates the client with specified user and password.
//
// "anonymous"/"anonymous" is a common user/password scheme for FTP servers
// that allows anonymous read-only accounts.
func (c *ServerConn) Login(ctx context.Context, user, password string) error {
	code, message, err := c.cmd(ctx, -1, "USER %s", user)
	if err != nil {
		return err
	}

	switch code {
	case StatusLoggedIn:
	case StatusUserOK:
		_, _, err = c.cmd(ctx, StatusLoggedIn, "PASS %s", password)
		if err != nil {
			return err
		}
	default:
		return errors.New(message)
	}

	// Switch to binary mode
	if _, _, err = c.cmd(ctx, StatusCommandOK, "TYPE I"); err != nil {
		return err
	}

	// Switch to UTF-8
	err = c.setUTF8(ctx)

	// If using implicit TLS, make data connections also use TLS
	if c.options.tlsConfig != nil {
		c.cmd(ctx, StatusCommandOK, "PBSZ 0")
		c.cmd(ctx, StatusCommandOK, "PROT P")
	}

	return err
}

// feat issues a FEAT FTP command to list the additional commands supported by
// the remote FTP server.
// FEAT is described in RFC 2389
func (c *ServerConn) feat(ctx context.Context) error {
	code, message, err := c.cmd(ctx, -1, "FEAT")
	if err != nil {
		return err
	}

	if code != StatusSystem {
		// The server does not support the FEAT command. This is not an
		// error: we consider that there is no additional feature.
		return nil
	}

	lines := strings.Split(message, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, " ") {
			continue
		}

		line = strings.TrimSpace(line)
		featureElements := strings.SplitN(line, " ", 2)

		command := featureElements[0]

		var commandDesc string
		if len(featureElements) == 2 {
			commandDesc = featureElements[1]
		}

		c.features[command] = commandDesc
	}

	return nil
}

// setUTF8 issues an "OPTS UTF8 ON" command.
func (c *ServerConn) setUTF8(ctx context.Context) error {
	if _, ok := c.features["UTF8"]; !ok {
		return nil
	}

	code, message, err := c.cmd(ctx, -1, "OPTS UTF8 ON")
	if err != nil {
		return err
	}

	// Workaround for FTP servers, that does not support this option.
	if code == StatusBadArguments {
		return nil
	}

	// The ftpd "filezilla-server" has FEAT support for UTF8, but always returns
	// "202 UTF8 mode is always enabled. No need to send this command." when
	// trying to use it. That's OK
	if code == StatusCommandNotImplemented {
		return nil
	}

	if code != StatusCommandOK {
		return errors.New(message)
	}

	return nil
}

// epsv issues an "EPSV" command to get a port number for a data connection.
func (c *ServerConn) epsv(ctx context.Context) (port int, err error) {
	_, line, err := c.cmd(ctx, StatusExtendedPassiveMode, "EPSV")
	if err != nil {
		return
	}

	start := strings.Index(line, "|||")
	end := strings.LastIndex(line, "|")
	if start == -1 || end == -1 {
		err = errors.New("invalid EPSV response format")
		return
	}
	port, err = strconv.Atoi(line[start+3 : end])
	return
}

// pasv issues a "PASV" command to get a port number for a data connection.
func (c *ServerConn) pasv(ctx context.Context) (host string, port int, err error) {
	_, line, err := c.cmd(ctx, StatusPassiveMode, "PASV")
	if err != nil {
		return
	}

	// PASV response format : 227 Entering Passive Mode (h1,h2,h3,h4,p1,p2).
	start := strings.Index(line, "(")
	end := strings.LastIndex(line, ")")
	if start == -1 || end == -1 {
		err = errors.New("invalid PASV response format")
		return
	}

	// We have to split the response string
	pasvData := strings.Split(line[start+1:end], ",")

	if len(pasvData) < 6 {
		err = errors.New("invalid PASV response format")
		return
	}

	// Let's compute the port number
	portPart1, err1 := strconv.Atoi(pasvData[4])
	if err1 != nil {
		err = err1
		return
	}

	portPart2, err2 := strconv.Atoi(pasvData[5])
	if err2 != nil {
		err = err2
		return
	}

	// Recompose port
	port = portPart1*256 + portPart2

	// Make the IP address to connect to
	host = strings.Join(pasvData[0:4], ".")
	return
}

// getDataConnPort returns a host, port for a new data connection
// it uses the best available method to do so
func (c *ServerConn) getDataConnPort(ctx context.Context) (string, int, error) {
	if !c.options.disableEPSV && !c.skipEPSV {
		if port, err := c.epsv(ctx); err == nil {
			return c.host, port, nil
		}

		// if there is an error, skip EPSV for the next attempts
		c.skipEPSV = true
	}

	return c.pasv(ctx)
}

// openDataConn creates a new FTP data connection.
func (c *ServerConn) openDataConn(ctx context.Context) (net.Conn, error) {
	host, port, err := c.getDataConnPort(ctx)
	if err != nil {
		return nil, err
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	if c.options.dialFunc != nil {
		return c.options.dialFunc(ctx, "tcp", addr)
	}

	if c.options.tlsConfig != nil {
		conn, err := c.options.dialer.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		return tls.Client(conn, c.options.tlsConfig), err
	}

	return c.options.dialer.Dial("tcp", addr)
}

// cmd is a helper function to execute a command and check for the expected FTP
// return code
func (c *ServerConn) cmd(ctx context.Context, expected int, format string, args ...interface{}) (int, string, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			c.conn.Close()
		case <-done:
		}
	}()
	_, err := c.conn.Cmd(format, args...)
	if err != nil {
		return 0, "", err
	}
	return c.conn.ReadResponse(expected)
}

// cmdDataConnFrom executes a command which require a FTP data connection.
// Issues a REST FTP command to specify the number of bytes to skip for the transfer.
func (c *ServerConn) cmdDataConnFrom(ctx context.Context, offset uint64, format string, args ...interface{}) (net.Conn, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			c.conn.Close()
		case <-done:
		}
	}()
	conn, err := c.openDataConn(ctx)
	if err != nil {
		return nil, err
	}

	if offset != 0 {
		_, _, err := c.cmd(ctx, StatusRequestFilePending, "REST %d", offset)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	_, err = c.conn.Cmd(format, args...)
	if err != nil {
		conn.Close()
		return nil, err
	}

	code, msg, err := c.conn.ReadResponse(-1)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if code != StatusAlreadyOpen && code != StatusAboutToSend {
		conn.Close()
		return nil, &textproto.Error{Code: code, Msg: msg}
	}

	return conn, nil
}

// resetDcTimeout restart timeout for a connection
func (c *ServerConn) resetDcTimeout(conn net.Conn) {
	if c.options.dcTimeout > 0 {
		conn.SetDeadline(time.Now().Add(c.options.dcTimeout))
	}
}

// NameList issues an NLST FTP command.
func (c *ServerConn) NameList(ctx context.Context, path string) (entries []string, err error) {
	conn, err := c.cmdDataConnFrom(ctx, 0, "NLST %s", path)
	if err != nil {
		return
	}

	r := &Response{conn: conn, c: c}
	defer r.Close()

	scanner := bufio.NewScanner(r)
	c.resetDcTimeout(conn)
	for scanner.Scan() {
		entries = append(entries, scanner.Text())
		c.resetDcTimeout(conn)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if err = scanner.Err(); err != nil {
		return entries, err
	}
	return
}

// List issues a LIST FTP command.
func (c *ServerConn) List(ctx context.Context, path string) (entries []*Entry, err error) {
	var cmd string
	var parser parseFunc

	if c.mlstSupported {
		cmd = "MLSD"
		parser = parseRFC3659ListLine
	} else {
		cmd = "LIST"
		parser = parseListLine
	}

	conn, err := c.cmdDataConnFrom(ctx, 0, "%s %s", cmd, path)
	if err != nil {
		return
	}

	r := &Response{conn: conn, c: c}
	defer r.Close()

	scanner := bufio.NewScanner(r)
	c.resetDcTimeout(conn)
	now := time.Now()
	for scanner.Scan() {
		entry, err := parser(scanner.Text(), now, c.options.location)
		if err == nil {
			entries = append(entries, entry)
		}
		c.resetDcTimeout(conn)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return
}

// ChangeDir issues a CWD FTP command, which changes the current directory to
// the specified path.
func (c *ServerConn) ChangeDir(ctx context.Context, path string) error {
	_, _, err := c.cmd(ctx, StatusRequestedFileActionOK, "CWD %s", path)
	return err
}

// ChangeDirToParent issues a CDUP FTP command, which changes the current
// directory to the parent directory.  This is similar to a call to ChangeDir
// with a path set to "..".
func (c *ServerConn) ChangeDirToParent(ctx context.Context) error {
	_, _, err := c.cmd(ctx, StatusRequestedFileActionOK, "CDUP")
	return err
}

// CurrentDir issues a PWD FTP command, which Returns the path of the current
// directory.
func (c *ServerConn) CurrentDir(ctx context.Context) (string, error) {
	_, msg, err := c.cmd(ctx, StatusPathCreated, "PWD")
	if err != nil {
		return "", err
	}

	start := strings.Index(msg, "\"")
	end := strings.LastIndex(msg, "\"")

	if start == -1 || end == -1 {
		return "", errors.New("unsuported PWD response format")
	}

	return msg[start+1 : end], nil
}

// FileSize issues a SIZE FTP command, which Returns the size of the file
func (c *ServerConn) FileSize(ctx context.Context, path string) (int64, error) {
	_, msg, err := c.cmd(ctx, StatusFile, "SIZE %s", path)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(msg, 10, 64)
}

// Retr issues a RETR FTP command to fetch the specified file from the remote
// FTP server.
//
// The returned ReadCloser must be closed to cleanup the FTP data connection.
func (c *ServerConn) Retr(ctx context.Context, path string) (Responser, error) {
	return c.RetrFrom(ctx, path, 0)
}

// RetrFrom issues a RETR FTP command to fetch the specified file from the remote
// FTP server, the server will not send the offset first bytes of the file.
//
// The returned ReadCloser must be closed to cleanup the FTP data connection.
func (c *ServerConn) RetrFrom(ctx context.Context, path string, offset uint64) (Responser, error) {
	conn, err := c.cmdDataConnFrom(ctx, offset, "RETR %s", path)
	if err != nil {
		return nil, err
	}

	return &Response{conn: conn, c: c}, nil
}

// Stor issues a STOR FTP command to store a file to the remote FTP server.
// Stor creates the specified file with the content of the io.Reader.
//
// Hint: io.Pipe() can be used if an io.Writer is required.
func (c *ServerConn) Stor(ctx context.Context, path string, r io.Reader) (code int, err error) {
	return c.StorFrom(ctx, path, r, 0)
}

// StorFrom issues a STOR FTP command to store a file to the remote FTP server.
// Stor creates the specified file with the content of the io.Reader, writing
// on the server will start at the given file offset.
//
// Hint: io.Pipe() can be used if an io.Writer is required.
func (c *ServerConn) StorFrom(ctx context.Context, path string, r io.Reader, offset uint64) (code int, err error) {
	conn, err := c.cmdDataConnFrom(ctx, offset, "STOR %s", path)
	if err != nil {
		return 0, err
	}
	_, err = io.Copy(conn, r)
	conn.Close()
	if err != nil {
		return 0, err
	}

	code, _, err = c.conn.ReadResponse(StatusClosingDataConnection)
	return code, err
}

// Rename renames a file on the remote FTP server.
// if code > 0 then it's not a connection/protocol error. It's a servere reply error like 553 file
// already exists
func (c *ServerConn) Rename(ctx context.Context, from, to string) (code int, err error) {
	code, _, err = c.cmd(ctx, StatusRequestFilePending, "RNFR %s", from)
	if err != nil {
		return code, err
	}
	code, _, err = c.cmd(ctx, StatusRequestedFileActionOK, "RNTO %s", to)
	return code, err
}

// Delete issues a DELE FTP command to delete the specified file from the
// remote FTP server.
func (c *ServerConn) Delete(ctx context.Context, path string) (code int, err error) {
	code, _, err = c.cmd(ctx, StatusRequestedFileActionOK, "DELE %s", path)
	return code, err
}

// RemoveDirRecur deletes a non-empty folder recursively using
// RemoveDir and Delete
func (c *ServerConn) RemoveDirRecur(ctx context.Context, path string) (code int, err error) {
	err = c.ChangeDir(ctx, path)
	if err != nil {
		return 0, err
	}
	currentDir, err := c.CurrentDir(ctx)
	if err != nil {
		return 0, err
	}

	entries, err := c.List(ctx, currentDir)
	if err != nil {
		return 0, err
	}

	for _, entry := range entries {
		if entry.Name != ".." && entry.Name != "." {
			if entry.Type == EntryTypeFolder {
				code, err = c.RemoveDirRecur(ctx, currentDir+"/"+entry.Name)
				if err != nil {
					return code, err
				}
			} else {
				code, err = c.Delete(ctx, entry.Name)
				if err != nil {
					return code, err
				}
			}
		}
	}
	err = c.ChangeDirToParent(ctx)
	if err != nil {
		return 0, err
	}
	code, err = c.RemoveDir(ctx, currentDir)
	return code, err
}

// MakeDir issues a MKD FTP command to create the specified directory on the
// remote FTP server.
func (c *ServerConn) MakeDir(ctx context.Context, path string) (code int, err error) {
	code, _, err = c.cmd(ctx, StatusPathCreated, "MKD %s", path)
	return code, err
}

// RemoveDir issues a RMD FTP command to remove the specified directory from
// the remote FTP server.
func (c *ServerConn) RemoveDir(ctx context.Context, path string) (code int, err error) {
	code, _, err = c.cmd(ctx, StatusRequestedFileActionOK, "RMD %s", path)
	return code, err
}

// NoOp issues a NOOP FTP command.
// NOOP has no effects and is usually used to prevent the remote FTP server to
// close the otherwise idle connection.
func (c *ServerConn) NoOp(ctx context.Context) error {
	_, _, err := c.cmd(ctx, StatusCommandOK, "NOOP")
	return err
}

// Logout issues a REIN FTP command to logout the current user.
func (c *ServerConn) Logout(ctx context.Context) error {
	_, _, err := c.cmd(ctx, StatusReady, "REIN")
	return err
}

// Quit issues a QUIT FTP command to properly close the connection from the
// remote FTP server.
func (c *ServerConn) Quit() error {
	c.conn.Cmd("QUIT")
	return c.conn.Close()
}

// Read implements the io.Reader interface on a FTP data connection.
func (r *Response) Read(buf []byte) (int, error) {
	return r.conn.Read(buf)
}

// Close implements the io.Closer interface on a FTP data connection.
// After the first call, Close will do nothing and return nil.
func (r *Response) Close() error {
	if r.closed {
		return nil
	}
	err := r.conn.Close()
	_, _, err2 := r.c.conn.ReadResponse(StatusClosingDataConnection)
	if err2 != nil {
		err = err2
	}
	r.closed = true
	return err
}

// SetDeadline sets the deadlines associated with the connection.
func (r *Response) SetDeadline(t time.Time) error {
	return r.conn.SetDeadline(t)
}
