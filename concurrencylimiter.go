// Package bottleneck provides facilities to limit the number of concurrent active processes.
// It can be used for example to limit the number of concurrent requests
// processed by a server.
package bottleneck

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	random "math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/atdiar/xhttp"
)

var (
	Receipt      = NewTicket("receipt", "Ok")
	LosingTicket = NewTicket("void", "void")

	DefaultWinningTicketGenerator = func(a Authority) Ticket {
		p := Receipt
		for !a.Valid(p) {
			p = NewTicket(a.ID, RandomString(10))
		}
		return p
	}

	NewTicketEndpoint      = "/newticket"
	ExchangeTicketEndpoint = "/exchangeticket"
	NewBottleneckEndpoint  = "/newbottleneck" // should usually be kept private or protected via rbac etc.
)

type Ticket struct {
	AuthorityID string
	Value       string
}

func NewTicket(name string, value string) Ticket {
	return Ticket{name, value}
}

// Authority generates and keep a  winning ticket alive.
// A process will try to exchange its losing ticket for a winning one.
type Authority struct {
	mu             *sync.Mutex
	ID             string
	WinningTicket  Ticket
	Content        Ticket
	Receipt        Ticket
	MaxAge         int
	renewAfter     time.Time
	newticket      func(Authority) Ticket // creates a winning ticket
	expiredtickets set
}

// NewAuthority creates a authority with a ticket inside.
// Important note :  the initial ticket should not be a receipt.
// The receipt is the value that is solely returned  upon successfully returning
// a non-expired ticket.
func NewAuthority(id string, maxage int, ticket Ticket, ticketcreator func(Authority) Ticket) *Authority {
	return &Authority{&sync.Mutex{}, id, ticket, ticket, Receipt, maxage, time.Now().UTC().Add(time.Duration(maxage)), ticketcreator, newSet().Add(Receipt)}
}

// ExchangeOrReturnAttempt is used to either try to acquire the genuine ticket that
// is contained within the authority or return the ticket in which case a Receipt is
// given in return if the ticket is still valid.
// If the ticket has expired, it is not accepted and a fake one is returned.
func (b *Authority) ExchangeOrReturnAttempt(t Ticket) Ticket {
	b.mu.Lock()
	defer b.mu.Unlock()
	if res := time.Now().UTC().After(b.renewAfter); res {
		// if the ticket was outside of its authority (and consequently got replaced with
		// a fake ticket), then this  ticket has expired. A new ticket is created and
		// the old one is flagged into the set of expired tickets.
		if b.Content != b.WinningTicket {
			b.expiredtickets = b.expiredtickets.Add(b.WinningTicket)
			b.WinningTicket = b.newticket(*b)
			b.Content = b.WinningTicket
			b.resetExpiry()
		}
		// Otherwise if the ticket is stil in its authority, then it has not really expired
		b.resetExpiry()
	}

	if t == Receipt {
		return t // receipt cannot be exchanged. user needs to request for another ticket first (which will be a losing one evidently) and then resubmit it.
	}

	// If the ticket is being returned but has expired
	if b.expiredtickets.Contains(t) {
		return LosingTicket // we do not accept defective items here Sir ! Try again.
	}

	// if the ticket is being returned safe and sound :  nice, someone else will be able to use it
	if t == b.WinningTicket {
		b.Content = t
		b.resetExpiry()
		return b.Receipt
	}

	//  Otherwise, the user is just trying to acquire the ticket value, operating the great switch
	content := b.Content
	b.Content = t
	b.resetExpiry()

	return content
}

func (b *Authority) resetExpiry() {
	b.renewAfter = time.Now().UTC().Add(time.Duration(b.MaxAge))
}

func (a *Authority) Valid(p Ticket) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.expiredtickets.Contains(p)
}

// getLosingTicket generates the initial token that a process must get before even
// attempting to get a real one.
// TODO generate random number: the goal is for processes to get a real ticket
// which gives them tickets to perform an action provided they return Thte Token
// early enough
func getLosingTicket() (t Ticket) {
	return LosingTicket
}

type Bottleneck struct {
	Index                map[string]int
	Width                []*Authority
	ticketsMaxage        int
	Authorityidgenerator func() string
	Ticketgenerator      func(Authority) Ticket
}

func NewBottleneck(tickettimelimit int) Bottleneck {
	return Bottleneck{nil, nil, tickettimelimit, func() string { return RandomString(16) }, DefaultWinningTicketGenerator}
}

func (b Bottleneck) SetAuthorityIDgenerator(f func() string) Bottleneck {
	b.Authorityidgenerator = f
	return b
}

func (b Bottleneck) SetTicketgenerator(f func(Authority) Ticket) Bottleneck {
	b.Ticketgenerator = f
	return b
}

func (c Bottleneck) Generate(maxconcurrency int) Bottleneck {
	c.Index = make(map[string]int, maxconcurrency)
	c.Width = make([]*Authority, maxconcurrency)
	for i, _ := range c.Width {
		id := c.Authorityidgenerator()
		c.Width[i] = NewAuthority(id, c.ticketsMaxage, Receipt, c.Ticketgenerator)
		c.Index[id] = i
	}
	return c
}

// NewTicket deals anyone who request it with a losing Ticket, that is, a
// ticket that does not grant the right for a process to proceed further on.
// The goal of the process ought to be to try to exchange it against one of the
// bottleneck winning Ticket issued and kept by one of the bottleneck authorities.
//
// With the winningticket in hand, a process shall be able to proceed as long as the
// ticket has not expired. After it's done, the process should try and return the
// winning ticket, exchanging it in hopes to receive a receipt.
// The receipt cannot be exchanged. If the process needs to  start again after
// an initial success and the reception of the receipt, it needs to request for
// a new ticket via the NewParticipant method.
func (b Bottleneck) NewTicket() Ticket {
	return getLosingTicket()
}

func (b Bottleneck) ExchangeTicket(t Ticket) Ticket {
	if t == Receipt {
		return t // receipts cannot be exchanged
	}
	i, ok := b.Index[t.AuthorityID]
	if ok {
		return b.Width[i].ExchangeOrReturnAttempt(t) // it either returns a receipt or a losing ticket if expired.
	}
	if t != LosingTicket {
		return t // we do not accept fake tickets here, Sir!
	}
	for _, a := range b.Width {
		t = a.ExchangeOrReturnAttempt(t)
		if t != LosingTicket {
			return t
		}
	}
	return t
}

/*
type HttpServer struct {
	Name string
	mu   *sync.Mutex
	List map[string]Bottleneck

	*http.Server

	Session session.Handler
}

//// NewHTTPServer returns a new http service that can be used to orchestrate
// and restrict the number of concurrent requests.
func NewHTTPServer(name string, secret string) HttpServer {
	Session := session.New(name, secret)
	x := xhttp.NewServeMux() // todo see if the server needs to be initialized with rbac
	hs := new(http.Server)
	Session.Log = hs.ErrorLog
	s := HttpServer{name, new(sync.Mutex), make(map[string]Bottleneck, 10), hs, Session}

	SessionEnforcer := xhttp.LinkableHandler(xhttp.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		ctx, err := Session.Load(ctx, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
		}
	}))

	namehandler := xhttp.LinkableHandler(xhttp.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(name))
	}))

	sendticket := xhttp.LinkableHandler(xhttp.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		ctx, err := Session.Load(ctx, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
			return
		}
		sid, err := Session.ID()
		if err != nil {
			http.Error(w, "Unable to retrieve session id", http.StatusInternalServerError)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
			return
		}
		tix, err := s.NewTicket(sid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = xhttp.WriteJSON(w, tix, http.StatusOK)
		if err != nil {
			http.Error(w, "Unable to send requesting ticket", http.StatusInternalServerError)
		}
	}))

	exchangeticket := xhttp.LinkableHandler(xhttp.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		ctx, err := Session.Load(ctx, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
			return
		}
		sid, err := Session.ID()
		if err != nil {
			http.Error(w, "Unable to retrieve session id", http.StatusInternalServerError)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
			return
		}

		var t Ticket

		// Let's Parse the request body , hoping ot retroieve the submitted ticket

		if r.Header.Get("Content-Type") != "" {
			contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if contentType != "application/json" {
				http.Error(w, "Content-Type header is not application/json", http.StatusUnsupportedMediaType)
				return
			}
		}

		// Use http.MaxBytesReader to enforce a maximum read of 1MB from the
		// response body. A request body larger than that will now result in
		// Decode() returning a "http: request body too large" error.
		r.Body = http.MaxBytesReader(w, r.Body, 1024<<10)

		// Setup the decoder and call the DisallowUnknownFields() method on it.
		// This will cause Decode() to return a "json: unknown field ..." error
		// if it encounters any extra unexpected fields in the JSON. Strictly
		// speaking, it returns an error for "keys which do not match any
		// non-ignored, exported fields in the destination".
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		err = dec.Decode(&t)
		if err != nil {
			var syntaxError *json.SyntaxError
			var unmarshalTypeError *json.UnmarshalTypeError

			switch {
			// Catch any syntax errors in the JSON and send an error message
			// which interpolates the location of the problem to make it
			// easier for the client to fix.
			case errors.As(err, &syntaxError):
				http.Error(w, fmt.Sprintf("Request body contains badly-formed JSON (at position %d)", syntaxError.Offset), http.StatusBadRequest)

			case errors.Is(err, io.ErrUnexpectedEOF):
				http.Error(w, fmt.Sprintf("Request body contains badly-formed JSON"), http.StatusBadRequest)

			// Catch any type errors, like trying to assign a string in the
			// JSON request body to a int field in our Person struct.
			case errors.As(err, &unmarshalTypeError):
				http.Error(w, fmt.Sprintf("Request body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset), http.StatusBadRequest)

			// Catch the error caused by extra unexpected fields in the request
			// body. We extract the field name from the error message and
			// interpolate it in our custom error message.
			case strings.HasPrefix(err.Error(), "json: unknown field "):
				fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
				http.Error(w, fmt.Sprintf("Request body contains unknown field %s", fieldName), http.StatusBadRequest)

			// An io.EOF error is returned by Decode() if the request body is
			// empty.
			case errors.Is(err, io.EOF):
				http.Error(w, "Request body must not be empty", http.StatusBadRequest)

			// Catch the error caused by the request body being too large.
			case err.Error() == "http: request body too large":
				http.Error(w, "Request body must not be larger than 1MB", http.StatusRequestEntityTooLarge)

			// Otherwise default to logging the error and sending a 500 Internal
			// Server Error response.
			default:
				if s.Server.ErrorLog != nil {
					s.Server.ErrorLog.Print(err)
				}
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
			return
		}

		// Call decode again, using a pointer to an empty anonymous struct as
		// the destination. If the request body only contained a single JSON
		// object this will return an io.EOF error. So if we get anything else,
		// we know that there is additional data in the request body.
		err = dec.Decode(&struct{}{})
		if err != io.EOF {
			http.Error(w, "Request body must only contain a single JSON object", http.StatusBadRequest)
			return
		}

		tix, err := s.ExchangeTicket(sid, t)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = xhttp.WriteJSON(w, tix, http.StatusOK)
		if err != nil {
			http.Error(w, "Unable to send requesting ticket", http.StatusInternalServerError)
		}
	}))

	newbottleneck := xhttp.LinkableHandler(xhttp.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		ctx, err := Session.Load(ctx, w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
			return
		}
		sid, err := Session.ID()
		if err != nil {
			http.Error(w, "Unable to retrieve session id", http.StatusInternalServerError)
			cancelFn, ok := ctx.Value(xhttp.CancelingKey).(context.CancelFunc)
			if ok {
				cancelFn() // checking but given the fact that a LinkableHandler always uses a cancelabloe context and store the cancel function in the context, there should not be a problem.
			}
			return
		}
		t := struct {
			maxage         int
			maxconcurrency int
		}{0, 0}
		if r.Header.Get("Content-Type") != "" {
			contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if contentType != "application/json" {
				http.Error(w, "Content-Type header is not application/json", http.StatusUnsupportedMediaType)
				return
			}
		}

		// Use http.MaxBytesReader to enforce a maximum read of 1MB from the
		// response body. A request body larger than that will now result in
		// Decode() returning a "http: request body too large" error.
		r.Body = http.MaxBytesReader(w, r.Body, 1024<<10)

		// Setup the decoder and call the DisallowUnknownFields() method on it.
		// This will cause Decode() to return a "json: unknown field ..." error
		// if it encounters any extra unexpected fields in the JSON. Strictly
		// speaking, it returns an error for "keys which do not match any
		// non-ignored, exported fields in the destination".
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		err = dec.Decode(&t)
		if err != nil {
			var syntaxError *json.SyntaxError
			var unmarshalTypeError *json.UnmarshalTypeError

			switch {
			// Catch any syntax errors in the JSON and send an error message
			// which interpolates the location of the problem to make it
			// easier for the client to fix.
			case errors.As(err, &syntaxError):
				http.Error(w, fmt.Sprintf("Request body contains badly-formed JSON (at position %d)", syntaxError.Offset), http.StatusBadRequest)

			case errors.Is(err, io.ErrUnexpectedEOF):
				http.Error(w, fmt.Sprintf("Request body contains badly-formed JSON"), http.StatusBadRequest)

			// Catch any type errors, like trying to assign a string in the
			// JSON request body to a int field in our Person struct.
			case errors.As(err, &unmarshalTypeError):
				http.Error(w, fmt.Sprintf("Request body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset), http.StatusBadRequest)

			// Catch the error caused by extra unexpected fields in the request
			// body. We extract the field name from the error message and
			// interpolate it in our custom error message.
			case strings.HasPrefix(err.Error(), "json: unknown field "):
				fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
				http.Error(w, fmt.Sprintf("Request body contains unknown field %s", fieldName), http.StatusBadRequest)

			// An io.EOF error is returned by Decode() if the request body is
			// empty.
			case errors.Is(err, io.EOF):
				http.Error(w, "Request body must not be empty", http.StatusBadRequest)

			// Catch the error caused by the request body being too large.
			case err.Error() == "http: request body too large":
				http.Error(w, "Request body must not be larger than 1MB", http.StatusRequestEntityTooLarge)

			// Otherwise default to logging the error and sending a 500 Internal
			// Server Error response.
			default:
				if s.Server.ErrorLog != nil {
					s.Server.ErrorLog.Print(err)
				}
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
			return
		}

		// Call decode again, using a pointer to an empty anonymous struct as
		// the destination. If the request body only contained a single JSON
		// object this will return an io.EOF error. So if we get anything else,
		// we know that there is additional data in the request body.
		err = dec.Decode(&struct{}{})
		if err != io.EOF {
			http.Error(w, "Request body must only contain a single JSON object", http.StatusBadRequest)
			return
		}

		s.NewBottleneck(sid, t.maxage, t.maxconcurrency)
	}))

	x.GET("/authenticate", xhttp.Chain(Session))                                 // TODO Remove : this is just for develoment purposes here but optimally, a central authentication authority should be used for authorization.
	x.GET("/name", xhttp.Chain(SessionEnforcer, namehandler))                    // could be a discovery endpoint sending the available public endpoint of the rest api
	x.GET(NewTicketEndpoint, xhttp.Chain(SessionEnforcer, sendticket))           // to get a ticket
	x.POST(ExchangeTicketEndpoint, xhttp.Chain(SessionEnforcer, exchangeticket)) // to exchange or return ticket
	x.POST(NewBottleneckEndpoint, xhttp.Chain(SessionEnforcer, newbottleneck))   // to create a new bottleneck

	hs.Handler = x.ServeMux

	return s
}

func (h HttpServer) NewBottleneck(bottleneckID string, ticketmaxage int, maxconcurrency int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.List[bottleneckID]; !ok {
		h.List[bottleneckID] = NewBottleneck(ticketmaxage).Generate(maxconcurrency)
	}
	if h.Server.ErrorLog != nil {
		h.Server.ErrorLog.Print(fmt.Sprintf("Attempt to create a bottleneck failed. Id %s with maxage= %v and concurrency limit= %v does already exist.", bottleneckID, ticketmaxage, maxconcurrency))
	}
}

func (h HttpServer) NewTicket(bottleneckID string) (Ticket, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.List[bottleneckID]; !ok {
		return Receipt, errors.New("bottleneckID does not exist.")
	}
	return h.List[bottleneckID].NewTicket(), nil
}

func (h HttpServer) ExchangeTicket(bottleneckID string, t Ticket) (Ticket, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.List[bottleneckID]; !ok {
		return Receipt, errors.New("bottleneckID does not exist")
	}
	return h.List[bottleneckID].ExchangeTicket(t), nil
}
*/

type RPCHandler struct {
	mu          *sync.Mutex
	Bottlenecks map[string]Bottleneck
}

func NewRPCHandler() RPCHandler {
	r := RPCHandler{}

	r.mu = &sync.Mutex{}
	r.Bottlenecks = make(map[string]Bottleneck)

	return r
}

func (r RPCHandler) NewBottleneck(req CreateRequest, res *string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.Bottlenecks[req.BottleneckID]; !ok {
		r.Bottlenecks[req.BottleneckID] = NewBottleneck(req.TicketMaxage).Generate(req.MaxConcurrency)
		return nil
	}
	*res = "error"
	return errors.New(fmt.Sprintf("Attempt to create a bottleneck failed. Id %s with maxage= %v and concurrency limit= %v does already exist.", req.BottleneckID, req.TicketMaxage, req.MaxConcurrency))
}

func (r RPCHandler) NewTicket(req TicketRequest, res *Ticket) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.Bottlenecks[req.BottleneckID]; !ok {
		*res = Receipt
		return errors.New("bottleneckID does not exist.")
	}
	*res = r.Bottlenecks[req.BottleneckID].NewTicket()
	return nil
}

func (r RPCHandler) ExchangeTicket(req ExchangeTicketRequest, res *Ticket) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.Bottlenecks[req.BottleneckID]; !ok {
		*res = Receipt
		return errors.New("bottleneckID does not exist.")
	}
	*res = r.Bottlenecks[req.BottleneckID].ExchangeTicket(req.Ticket)
	return nil
}

type CreateRequest struct {
	BottleneckID   string
	TicketMaxage   int
	MaxConcurrency int
}

func NewBottleneckRequest(bottleneckid string, maxage int, maxconcurrency int) CreateRequest {
	return CreateRequest{bottleneckid, maxage, maxconcurrency}
}

type TicketRequest struct {
	BottleneckID string
}

func NewTicketRequest(bottleneckid string) TicketRequest { return TicketRequest{bottleneckid} }

type ExchangeTicketRequest struct {
	BottleneckID string
	Ticket       Ticket
}

func NewExchangeTicketRequest(bottleneckid string, t Ticket) ExchangeTicketRequest {
	return ExchangeTicketRequest{bottleneckid, t}
}

type Server struct {
	ID         string
	RPC        *rpc.Server
	Middleware xhttp.HandlerLinker
	Mux        xhttp.ServeMux

	Path string

	*http.Server
}

func NewRPCServer(serverid string, path string, configs ...func(Server) Server) Server {
	var s Server
	var err error
	s.ID = serverid
	s.RPC = rpc.NewServer()

	s.Path = path
	s.Mux = xhttp.NewServeMux()

	s.Server = &http.Server{}

	for _, config := range configs {
		s = config(s)
	}

	err = s.RPC.RegisterName("rpc", NewRPCHandler())
	if err != nil {
		panic(err)
	}
	s.Mux.CONNECT(s.Path, s.Middleware.Link(xhttp.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		s.RPC.ServeHTTP(w, r) // it is used on initial dialup to hijack the connection and serve the RPC response
	})))

	s.Server.Handler = s.Mux

	return s
}

func WithMiddleware(h xhttp.HandlerLinker) func(Server) Server {
	return func(s Server) Server {
		s.Middleware = h
		return s
	}
}

func WithMux(m xhttp.ServeMux) func(Server) Server {
	return func(s Server) Server {
		s.Mux = m
		return s
	}
}

func WithServer(s *http.Server) func(Server) Server {
	return func(se Server) Server {
		se.Server = s
		return se
	}
}

//
//
//

type Client struct {
	RPC     *rpc.Client
	Connect *http.Request

	Network string
	Addr    string
	Path    string
}

func (c Client) NewBottleneck(bottleneckid string, maxage int, maxconcurrency int) error {
	var res string
	err := c.RPC.Call("rpc.NewBottleneck", NewBottleneckRequest(bottleneckid, maxage, maxconcurrency), &res)
	return err
}

func (c Client) NewTicket(bottleneckid string) (Ticket, error) {
	var t Ticket
	err := c.RPC.Call("rpc.NewTicket", NewTicketRequest(bottleneckid), &t)
	return t, err
}

func (c Client) ExchangeTicket(bottleneckid string, t Ticket) (Ticket, error) {
	var nt Ticket
	err := c.RPC.Call("rpc.ExchangeTicket", NewExchangeTicketRequest(bottleneckid, t), &nt)
	return nt, err
}

func NewClient(ctx context.Context, address, path string, configs ...func(Client) Client) (Client, error) {
	var c Client
	var err error
	c.Addr = address
	c.Path = path
	c.Network = "tcp"
	c.Connect, err = http.NewRequestWithContext(ctx, "CONNECT", c.Addr, nil)
	if err != nil {
		return c, err
	}

	for _, config := range configs {
		c = config(c)
	}

	conn, err := net.Dial(c.Network, c.Addr)
	if err != nil {
		return c, err
	}

	io.WriteString(conn, "CONNECT "+c.Path+" HTTP/1.0\n\n")

	resp, err := http.ReadResponse(bufio.NewReader(conn), c.Connect)
	if err == nil && resp.Status == "200 Connected to Go RPC" {
		c.RPC = rpc.NewClient(conn)
		return c, nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return c, &net.OpError{
		Op:   "dial-http",
		Net:  c.Network + " " + c.Addr,
		Addr: nil,
		Err:  err,
	}

	return c, err
}

func WithConnectRequest(r *http.Request) func(Client) Client {
	return func(c Client) Client {
		c.Connect = r
		return c
	}
}

// Todo create methods returning  xhttp request handlers
//

// set defines an unordered list of integer elements.
type set map[Ticket]bool

func newSet() set {
	s := make(map[Ticket]bool)
	return s
}

func (s set) Add(tickets ...Ticket) set {
	for _, t := range tickets {
		s[t] = true
	}
	return s
}

func (s set) Remove(t Ticket) {
	delete(s, t)
}

func (s set) Contains(t Ticket) bool {
	return s[t]
}

func (s set) Includes(v set) bool {
	for k := range v {
		if !s[k] {
			return false
		}
	}
	return true
}

func (s set) Union(v set) set {
	r := newSet()
	for k := range v {
		r.Add(k)
	}
	for k := range s {
		r.Add(k)
	}
	return r
}

func (s set) Inter(v set) set {
	r := newSet()
	for k := range v {
		if s[k] {
			r.Add(k)
		}
	}
	return r
}

func (s set) List() []Ticket {
	l := make([]Ticket, len(s))
	i := 0
	for k := range s {
		l[i] = k
		i++
	}
	return l
}

func (s set) Count() int {
	return len(s)
}

// RandomString creates a base64 encoded version of a 32byte Cryptographically
// secure random number .
// It uses Go's implementation of devurandom (which has a backup in case
// devurandom is inaccessible)
func RandomString(length int) string {
	bstr := make([]byte, length)
	_, err := rand.Read(bstr)
	if err != nil {
		random.Seed(time.Now().UnixNano())
		_, _ = random.Read(bstr)
	}
	return base64.StdEncoding.EncodeToString(bstr)
}
