package radix

import "sync"

type connPipelinerOpts struct {
	windowSize int
}

// ConnPipelinerOpt is an option which can be applied to a ConnPipeliner to
// effect how it functions.
type ConnPipelinerOpt func(*connPipelinerOpts)

// ConnPipelinerWindowSize describes the max number of messages which will be
// buffered in the ConnPipeliner before it flushes them out. A larger window
// size will result in fewer round-trips with the server, but more of a delay
// between the round-trips.
//
// Defaults to 10. TODO optimize this number
func ConnPipelinerWindowSize(size int) ConnPipelinerOpt {
	return func(opts *connPipelinerOpts) {
		opts.windowSize = size
	}
}

type connPipelinerAction struct {
	action   Action
	returned bool
	resCh    chan error
}

func (cpa *connPipelinerAction) sendRes(err error) {
	if !cpa.returned {
		cpa.resCh <- err
		cpa.returned = true
	}
}

// ConnPipeliner wraps a Conn such that it will buffer EncodeDecode calls until
// certain thresholds are met (either time or buffer size). At that point it
// will perform all encodes, in order, in a single write, then perform all
// decodes, in order, in a single read.
//
// ConnPipeliner's methods are thread-safe.
type ConnPipeliner struct {
	conn     pipelineConn
	opts     connPipelinerOpts
	actionCh chan connPipelinerAction

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeCh   chan struct{}
}

var _ Client = new(ConnPipeliner)

// NewConnPipeliner initializes and returns a ConnPipeliner wrapping the given
// Conn.
func NewConnPipeliner(conn Conn, opts ...ConnPipelinerOpt) *ConnPipeliner {
	cpo := connPipelinerOpts{
		windowSize: 10,
	}
	for _, opt := range opts {
		opt(&cpo)
	}

	cp := &ConnPipeliner{
		conn: pipelineConn{
			Conn: conn,
			mm:   make([]pipelineMarshalerUnmarshaler, 0, cpo.windowSize*2),
		},
		opts:     cpo,
		actionCh: make(chan connPipelinerAction, 16),
		closeCh:  make(chan struct{}),
	}

	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		cp.spin()
	}()

	return cp
}

// Unwrap is like Close, except it will not close the underlying Conn which the
// ConnPipeliner is wrapping, but returns it instead. It cleans up all other
// resources held by the ConnPipeliner, and the ConnPipeliner should not be used
// again after this returns.
//
// TODO I'm not sure if this method is useful, but it might be for Pool
// TODO figure out how this interacts with Flush
func (cp *ConnPipeliner) Unwrap() Conn {
	cp.closeOnce.Do(func() {
		close(cp.closeCh)
		cp.wg.Wait()
	})
	return cp.conn.Conn
}

// Close will clean up all resources held by this ConnPipeliner and close the
// Conn it wraps.
func (cp *ConnPipeliner) Close() error {
	return cp.Unwrap().Close()
}

func (cp *ConnPipeliner) maybeFlush() {
	if len(cp.conn.mm) < cp.opts.windowSize {
		return
	}

	cp.conn.flush()
	defer cp.conn.reset()

	for _, m := range cp.conn.mm {
		if m.err != nil {
			m.cpAction.sendRes(m.err)
		}
	}

	// once all errors are distributed, send out nil errors. sendRes will eat
	// the nil for any connPipelinerActions which have already had an
	// error written to them, but the rest will be informed that they were
	// accomplished successfully.
	for _, m := range cp.conn.mm {
		m.cpAction.sendRes(nil)
	}
}

func (cp *ConnPipeliner) spin() {
	for {
		select {
		case <-cp.closeCh:
			return
		case cpAction := <-cp.actionCh:
			cp.conn.nextCPAction = &cpAction
			if err := cpAction.action.Perform(&cp.conn); err != nil {
				cpAction.sendRes(err)
			}
			cp.maybeFlush()
		}
	}
}

// Do implements the method for the Client interface. It will batch the Action
// with the Actions of other concurrent Do calls, and perform them using a
// single write then a single read on the underlying connection (as if they were
// all part of a Pipeline).
func (cp *ConnPipeliner) Do(action Action) error {
	cpAction := connPipelinerAction{
		action: action,
		resCh:  make(chan error, 1),
	}
	cp.actionCh <- cpAction
	return <-cpAction.resCh
}
