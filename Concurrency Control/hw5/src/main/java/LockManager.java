import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }


    private HashMap<Resource, ResourceLock> resourceToLock;
    private HashMap<LockType, Integer> lockTypeToInt;
    private boolean[][] compatibilityMatrix;

    public LockManager() {
        this.resourceToLock = new HashMap<>();
        this.lockTypeToInt = new HashMap<>();
        this.lockTypeToInt.put(LockType.IS,0);
        this.lockTypeToInt.put(LockType.IX,1);
        this.lockTypeToInt.put(LockType.S, 2);
        this.lockTypeToInt.put(LockType.X, 3);
        setCompatibilityMatrix();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {

        // Throws IllegalArgumentException if transaction
        // tries to acquire an intent lock on a page
        if (transaction == null || resource == null || lockType == null) {
            throw new IllegalArgumentException("One of the parameters are null");
        }

        if (resource.getClass() == Page.class) {
            if (lockType.equals(LockType.IX) || lockType.equals(LockType.IS)) {
                throw new IllegalArgumentException("Cannot acquire an intent lock for a page");
            }
        }

        // Or a blocked transaction tries to acquire
        if (transaction.getStatus().equals(Transaction.Status.Waiting)) {
            throw new IllegalArgumentException("Blocked transaction cannot acquire a lock");
        }

        // Or already acquired this lock
        if (holds(transaction, resource, lockType)) {
            throw new IllegalArgumentException("Cannot acquire same lock");
        }

        // Or the parent table doesn't have an intent lock
        if (resource.getClass() == Page.class && !checkParentTableLock(resource, lockType)) {
            throw new IllegalArgumentException("Cannot grant lock: Parent table doesn't have an intent lock.");
        }

        //Or a downgrade lock is requested
        if (requestDowngrade(resource, lockType, transaction)) {
            throw new IllegalArgumentException("Cannot downgrade lock.");
        }


        Request currRequest = new Request(transaction, lockType);
        // If the resource has never been wanted, just grant a lock!
        if (!this.resourceToLock.containsKey(resource)) {
            ResourceLock resLock = new ResourceLock();
            resLock.lockOwners.add(currRequest);
            this.resourceToLock.put(resource, resLock);
        } else if (compatible(resource, transaction, lockType)) {
            // the lock is compatible, but only add when the transaction is not in lockOwners
            if (!this.resourceToLock.get(resource).lockOwners.contains(currRequest)) {
                this.resourceToLock.get(resource).lockOwners.add(currRequest);
            }
        } else {
            // the lock is incompatible with ALL transactions, but may be upgraded if the
            // only conflicting transaction is itself
            if (isUpgrade(resource, transaction, lockType)) {
                upgrade(resource, transaction, lockType);
            } else {
                transaction.sleep();
                this.resourceToLock.get(resource).requestersQueue.add(currRequest);
            }
        }
    }

    /**
     * Checks if a page's parent table has appropriate lock
     * @param resource input has to be a page
     * @return true if parent table has appropriate lock
     */
    private boolean checkParentTableLock(Resource resource, LockType lockType) {
        Page currPage = (Page) resource;
        Table parentTable = currPage.getTable();
        if (resourceToLock.containsKey(parentTable)) {
            ResourceLock currLock = resourceToLock.get(parentTable);
            for (Request r: currLock.lockOwners) {
                // page requests a S lock and parent table has IS/IX:
                if (r.lockType == LockType.IS || r.lockType == LockType.IX) {
                    if (lockType == LockType.S) {
                        return true;
                    } else if (r.lockType == LockType.IX) {
                        // page requests X lock and parent table has X
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Checks if a lock request is a downgrade
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     * @return true if request is a downgrade, false otherwise
     */
    private boolean requestDowngrade(Resource resource, LockType lockType, Transaction transaction) {
        Request requestIX = new Request(transaction, LockType.IX);
        Request requestX = new Request(transaction, LockType.X);
        if (!this.resourceToLock.containsKey(resource)) {
            return false;
        }
        if (lockType.equals(LockType.S) || lockType.equals(LockType.IS)) {
            ArrayList<Request> owners = this.resourceToLock.get(resource).lockOwners;
            if (owners.contains(requestIX) || owners.contains(requestX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method that constructs the compatibility matrix
     */
    private void setCompatibilityMatrix() {
        this.compatibilityMatrix = new boolean[4][4];

        this.compatibilityMatrix[0][0] = true;
        this.compatibilityMatrix[0][1] = true;
        this.compatibilityMatrix[0][2] = true;
        this.compatibilityMatrix[0][3] = false;

        this.compatibilityMatrix[1][0] = true;
        this.compatibilityMatrix[1][1] = true;
        this.compatibilityMatrix[1][2] = false;
        this.compatibilityMatrix[1][3] = false;

        this.compatibilityMatrix[2][0] = true;
        this.compatibilityMatrix[2][1] = false;
        this.compatibilityMatrix[2][2] = true;
        this.compatibilityMatrix[2][3] = false;

        this.compatibilityMatrix[3][0] = false;
        this.compatibilityMatrix[3][1] = false;
        this.compatibilityMatrix[3][2] = false;
        this.compatibilityMatrix[3][3] = false;
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        ResourceLock resLock = this.resourceToLock.get(resource);
        boolean allCompatible = true;
        for (Request r: resLock.lockOwners) {
            LockType existingLock = r.lockType;
            int requestLockIndex = this.lockTypeToInt.get(lockType);
            int existingLockIndex = this.lockTypeToInt.get(existingLock);
            if (!this.compatibilityMatrix[requestLockIndex][existingLockIndex]) {
                allCompatible = false;
            }
        }
        return allCompatible;
    }


    private boolean compatibleWithOthers(Resource resource, LockType lockType, Transaction transaction) {
        boolean compatible = true;
        Request requestIS = new Request(transaction, LockType.IS);
        Request requestS = new Request(transaction, LockType.S);
        for (Request r: this.resourceToLock.get(resource).lockOwners) {
            if (!((r.equals(requestS) && lockType == LockType.X)
                    || (r.equals(requestIS) && lockType == LockType.IX))){
                LockType existingLock = r.lockType;
                int requestLockIndex = this.lockTypeToInt.get(lockType);
                int existingLockIndex = this.lockTypeToInt.get(existingLock);
                if (!this.compatibilityMatrix[requestLockIndex][existingLockIndex]) {
                    compatible = false;
                }
            }
        }
        return compatible;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{

        if (transaction == null || resource == null) {
            throw new IllegalArgumentException("One of the arguments is null.");
        }

        // Blocked transaction tries to release a lock
        if (!holdsAnyLock(transaction, resource) && transaction.getStatus().equals(Transaction.Status.Waiting)) {
            throw new IllegalArgumentException("Blocked transaction cannot release a lock");
        }

        //A transaction without any lock tries to release
        if (!holdsAnyLock(transaction, resource)) {
            throw new IllegalArgumentException("Transaction doesn't hold any lock.");
        }

        //Trying to release table level lock before releasing all page level lock
        if (resource.getClass() == Table.class) {
            Table t = (Table) resource;
            for (Page p: t.getPages()) {
                if (holdsAnyLock(transaction, p)) {
                    throw new IllegalArgumentException("Cannot release a table level lock " +
                            " before release all page level locks");
                }
            }
        }

        //Release the lock by removing the transaction from the resource's lockOwners
        unlock(resource, transaction);
        promote(resource);


    }

    /**
     * Unlock the resource for the transaction by removing the transaction from
     * lockOwners
     * @param resource resource needs to be unlocked
     * @param transaction transaction that calls release
     */
    private void unlock(Resource resource, Transaction transaction) {
        ResourceLock resLock = this.resourceToLock.get(resource);
        Request requestToBeRemoved = null;
        for (Request r: resLock.lockOwners) {
            if (r.transaction.equals(transaction)) {
                requestToBeRemoved = r;
                break;
            }
        }
        resLock.lockOwners.remove(requestToBeRemoved);
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         //TODO: Be aware that the front of the queue may contain upgrade requests
         ArrayList<Request> owners = this.resourceToLock.get(resource).lockOwners;
         LinkedList<Request> waitingQueue = this.resourceToLock.get(resource).requestersQueue;
         LinkedList<Request> queueCopy = (LinkedList<Request>) waitingQueue.clone();
         for (Request r: queueCopy) {
             if (compatibleWithOthers(resource, r.lockType, r.transaction)) {
                 waitingQueue.remove(r);
                 r.transaction.wake();
                 if (isUpgrade(resource, r.transaction, r.lockType)) {
                     upgrade(resource, r.transaction, r.lockType);
                 } else {
                     owners.add(r);
                 }
             }
         }
     }

    /**
     * Checks if a transaction is requesting an upgrade
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if transaction is requesting upgrade; false otherwise
     */
     private boolean isUpgrade(Resource resource, Transaction transaction, LockType lockType) {
         Request requestIS = new Request(transaction, LockType.IS);
         Request requestS = new Request(transaction, LockType.S);

         if (this.resourceToLock.containsKey(resource)) {
             if (lockType.equals(LockType.X) || lockType.equals(LockType.IX)) {
                 ArrayList<Request> owners = this.resourceToLock.get(resource).lockOwners;

                 // Current transaction holds a lower grade lock. Want to upgrade
                 if (owners.contains(requestS)) {
                     return true;

                 } else if (owners.contains(requestIS)) {
                     return true;
                 }
             }
         }
         return false;
     }

    /**
     * Performs a lock upgrade if possible
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     */
     private void upgrade(Resource resource, Transaction transaction, LockType lockType) {
         Request requestS = new Request(transaction, LockType.S);
         Request newRequest = new Request(transaction, lockType);
         ArrayList<Request> owners = this.resourceToLock.get(resource).lockOwners;

         boolean compatible = compatibleWithOthers(resource, lockType, transaction);

         if (compatible) {
             if (owners.contains(requestS)) {
                 owners.remove(requestS);
                 owners.add(newRequest);
             }
         } else {
             LinkedList<Request> queue = this.resourceToLock.get(resource).requestersQueue;
             transaction.sleep();
             queue.addFirst(newRequest);
         }
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {

        if (transaction == null || resource == null || lockType == null) {
            throw new IllegalArgumentException("One of the arguments is null");
        }

        // The resource is not even requested before
        if (!resourceToLock.containsKey(resource)) {
            return false;
        }

        Request currRequest = new Request(transaction, lockType);
        // The lock owners contains the request
        if (resourceToLock.get(resource).lockOwners.contains(currRequest)) {
            return true;
        }

        return false;
    }

    /**
     * Returns true if the transaction holds one of the four locks
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @return true if a transaction holds one lock, false otherwise
     */
    private boolean holdsAnyLock(Transaction transaction, Resource resource) {
        return holds(transaction, resource, LockType.S)
                || holds(transaction, resource, LockType.IS)
                || holds(transaction, resource, LockType.IX)
                || holds(transaction, resource, LockType.X);
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }

}
