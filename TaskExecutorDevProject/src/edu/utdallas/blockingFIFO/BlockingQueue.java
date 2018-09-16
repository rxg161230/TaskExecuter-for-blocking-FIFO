package edu.utdallas.blockingFIFO;

import org.eclipse.jdt.annotation.Nullable;

/** A blocking queue.
 * 
 * <p>
 * This queue has the following properties:
 * <ul>
 *   <li>FIFO
 *   <li>unencapsulated
 *   <li>accepts null elements
 *   <li>bounded
 *   <li>fixed bound (set at initialization time)
 *   <li>thread-safe
 *   <li>blocking (on both <code>put</code> and <code>take</code> operations)
 * </ul>
 * 
 * @version 1.0.0, 2016-10-24
 * @since 1.0.0
 *
 * @param <E> type of element to store in this BlockingQueue
 */
public class BlockingQueue <E>
{
	private E[] queue;  // queue of type parameter E elements (cast to E upon return)
	private int capacity;  // upper bound on number of elements this queue may contain at any one time (effectively final)
	private int size;  // current number of elements in queue
	private int iNextPut;  // target index of next put operation
	private int iNextTake;  // target index of next take operation
	
	private Object notFull;  // monitor to signal when queue not full
	private Object notEmpty;  // monitor for signal when queue not empty
	private Object lockPut;  // synchronization lock for put method
	private Object lockTake;  // synchronization lock for take method
	
	/** Creates a new BlockingQueue with capacity (upper bound) fixed to the <code>capacity</code> argument.
	 * <code>capacity</code> argument is not validated. If negative, will result in an exception. If zero, may result in
	 * undefined behavior.
	 * 
	 * @param capacity the upper bound on the number of elements that this queue may contain at any one time
	 *        (effectively final)
	 * @throws NegativeArraySizeException if <code>capacity</code> argument is negative
	 */
	@SuppressWarnings("unchecked")
	public BlockingQueue(int capacity)
	{
		this.queue = (E[]) new Object[capacity];
		this.capacity = capacity;
		this.size = 0;
		this.iNextPut = 0;
		this.iNextTake = 0;
		
		this.notFull = new Object();
		this.notEmpty = new Object();
		this.lockPut = new Object();
		this.lockTake = new Object();
	}

	/** Enqueues the <code>elem</code> argument.  Unencapsulated.  Internally synchronized.  Argument may be
	 * <code>null</code>.
	 * 
	 * @param elem the element to enqueue, may be <code>null</code>
	 */
	public void put(@Nullable E elem)
	{
		synchronized (lockPut) {
			synchronized (notFull) {
				while (size == capacity) {
					try { notFull.wait(); } catch (InterruptedException e) { /* loop again */ }
				}
			}
			synchronized (this) {
				queue[iNextPut] = elem;
				iNextPut = ++iNextPut % capacity;
				++size;
			}
			synchronized (notEmpty) {
				notEmpty.notify();
			}
		}
	}
	
	/** Returns the least recently <code>put</code> element in this queue.  If the queue contains no elements, then this
	 * method will block until one or more elements are added (if this is the only "taker" thread, then exactly one).
	 * 
	 * <p>
	 * Unencapsulated.  Internally synchronized.  May return <code>null</code>.
	 * 
	 * @return the least recently <code>put</code> element in this queue.  May return <code>null</code> if a null
	 *         element was previously added.
	 */
	public @Nullable E take()
	{
		E elem;
		
		synchronized (lockTake) {
			synchronized (notEmpty) {
				while (size == 0) {
					try { notEmpty.wait(); } catch (InterruptedException e) { /* loop again */ }
				}
			}
			synchronized (this) {
				elem = queue[iNextTake];
				iNextTake = ++iNextTake % capacity;
				--size;
			}
			synchronized (notFull) {
				notFull.notify();
			}
		}
		
		return elem; 
	}
}
