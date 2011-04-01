/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.util.queue;

import org.mule.api.MuleContext;
import org.mule.api.context.MuleContextAware;
import org.mule.api.store.ListableObjectStore;
import org.mule.api.store.ObjectStore;
import org.mule.api.store.ObjectStoreException;
import org.mule.util.UUID;
import org.mule.util.store.SimpleMemoryObjectStore;
import org.mule.util.xa.AbstractTransactionContext;
import org.mule.util.xa.AbstractXAResourceManager;
import org.mule.util.xa.ResourceManagerException;
import org.mule.util.xa.ResourceManagerSystemException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;

/**
 * The Transactional Queue Manager is responsible for creating and Managing
 * transactional Queues. Queues can also be persistent by setting a persistence
 * strategy on the manager. Default straties are provided for Memory, Jounaling,
 * Cache and File.
 */
public class TransactionalQueueManager extends AbstractXAResourceManager implements QueueManager, MuleContextAware
{
    private Map<String, QueueInfo> queues = new HashMap<String, QueueInfo>();

    private ObjectStore<Serializable> memoryObjectStore = new SimpleMemoryObjectStore<Serializable>();
    private ListableObjectStore<Serializable> persistentObjectStore;

    private QueueConfiguration defaultQueueConfiguration = new QueueConfiguration(false);
    private MuleContext muleContext;

    public synchronized QueueSession getQueueSession()
    {
        return new TransactionalQueueSession(this, this);
    }

    public synchronized void setDefaultQueueConfiguration(QueueConfiguration config)
    {
        this.defaultQueueConfiguration = config;
    }

    public synchronized void setQueueConfiguration(String queueName, QueueConfiguration config)
    {
        getQueue(queueName).config = config;
    }

    protected synchronized QueueInfo getQueue(String name)
    {
        QueueInfo q = queues.get(name);
        if (q == null)
        {
            q = new QueueInfo();
            q.name = name;
            q.list = new LinkedList<Serializable>();
            q.config = defaultQueueConfiguration;

            queues.put(name, q);
        }
        return q;
    }

    @Override
    protected void doStart() throws ResourceManagerSystemException
    {
        if (persistentObjectStore != null)
        {
            try
            {
                persistentObjectStore.open();
            }
            catch (ObjectStoreException e)
            {
                throw new ResourceManagerSystemException(e);
            }
        }
    }

    @Override
    protected boolean shutdown(int mode, long timeoutMSecs)
    {
        try
        {
            if (persistentObjectStore != null)
            {
                persistentObjectStore.close();
            }
        }
        catch (ObjectStoreException e)
        {
            // TODO BL-405 what to do with this exception? Looking at the call graph of this method it seems that it's never called from any production code (i.e. when shutting down MuleContext)
            logger.error("Error closing persistent store", e);
        }

        // Clear queues on shutdown to avoid duplicate entries on warm restarts (MULE-3678)
        synchronized (this)
        {
            queues.clear();
        }
        return super.shutdown(mode, timeoutMSecs);
    }

    @Override
    protected void recover() throws ResourceManagerSystemException
    {
        if (persistentObjectStore != null)
        {
            try
            {
                List<Serializable> keys = persistentObjectStore.allKeys();
                for (Serializable key : keys)
                {
                    QueueKey queueKey = (QueueKey) key;
                    getQueue(queueKey.queueName).putNow(queueKey.id);
                }
            }
            catch (Exception e)
            {
                throw new ResourceManagerSystemException(e);
            }
        }
    }

    @Override
    protected AbstractTransactionContext createTransactionContext(Object session)
    {
        return new QueueTransactionContext(this);
    }

    @Override
    protected void doBegin(AbstractTransactionContext context)
    {
        // Nothing special to do
    }

    @Override
    protected int doPrepare(AbstractTransactionContext context)
    {
        return XAResource.XA_OK;
    }

    @Override
    protected void doCommit(AbstractTransactionContext context) throws ResourceManagerException
    {
        QueueTransactionContext ctx = (QueueTransactionContext) context;
        try
        {
            if (ctx.added != null)
            {
                for (Map.Entry<QueueInfo, List<Serializable>> entry : ctx.added.entrySet())
                {
                    QueueInfo queue = entry.getKey();
                    List<Serializable> queueAdded = entry.getValue();
                    if (queueAdded != null && queueAdded.size() > 0)
                    {
                        for (Serializable object : queueAdded)
                        {
                            Serializable id = doStore(queue, object);
                            queue.putNow(id);
                        }
                    }
                }
            }
            if (ctx.removed != null)
            {
                for (Map.Entry<QueueInfo, List<Serializable>> entry : ctx.removed.entrySet())
                {
                    QueueInfo queue = entry.getKey();
                    List<Serializable> queueRemoved = entry.getValue();
                    if (queueRemoved != null && queueRemoved.size() > 0)
                    {
                        for (Serializable id : queueRemoved)
                        {
                            doRemove(queue, id);
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw new ResourceManagerException(e);
        }
        finally
        {
            ctx.added = null;
            ctx.removed = null;
        }
    }

    protected Serializable doStore(QueueInfo queue, Serializable object) throws ObjectStoreException
    {
        ObjectStore<Serializable> store = queue.config.persistent ? persistentObjectStore : memoryObjectStore;

        String id = UUID.getUUID();
        Serializable key = new QueueKey(queue.name, id);
        store.store(key, object);
        return id;
    }

    protected void doRemove(QueueInfo queue, Serializable id) throws ObjectStoreException
    {
        ObjectStore<Serializable> store = queue.config.persistent ? persistentObjectStore : memoryObjectStore;

        Serializable key = new QueueKey(queue.name, id);
        store.remove(key);
    }

    protected Serializable doLoad(QueueInfo queue, Serializable id) throws ObjectStoreException
    {
        ObjectStore<Serializable> store = queue.config.persistent ? persistentObjectStore : memoryObjectStore;

        Serializable key = new QueueKey(queue.name, id);
        return store.retrieve(key);
    }

    @Override
    protected void doRollback(AbstractTransactionContext context) throws ResourceManagerException
    {
        QueueTransactionContext ctx = (QueueTransactionContext) context;
        if (ctx.removed != null)
        {
            for (Map.Entry<QueueInfo, List<Serializable>> entry : ctx.removed.entrySet())
            {
                QueueInfo queue = entry.getKey();
                List<Serializable> queueRemoved = entry.getValue();
                if (queueRemoved != null && queueRemoved.size() > 0)
                {
                    for (Serializable id : queueRemoved)
                    {
                        queue.putNow(id);
                    }
                }
            }
        }
        ctx.added = null;
        ctx.removed = null;
    }

    public void setPersistentObjectStore(ListableObjectStore<Serializable> store)
    {
        this.persistentObjectStore = store;
    }

    public void setMuleContext(MuleContext context)
    {
        this.muleContext = context;
    }

    public MuleContext getMuleContext()
    {
        return muleContext;
    }
}
