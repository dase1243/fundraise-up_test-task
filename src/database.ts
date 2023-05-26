import {Collection, Db, MongoClient} from "mongodb";
import dotenv from "dotenv";

dotenv.config();

const DB_URI: string = process.env.DB_URI || '';
const DB_NAME: string = 'ecommerce-store';
const DB_COLLECTION_CUSTOMERS: string = 'customers';
const DB_COLLECTION_ANONYMIZED: string = 'customers_anonymised';
const DB_COLLECTION_STATE: string = 'customers_anonymization_state';

async function connectToMongo(): Promise<Db> {
    const client: MongoClient = new MongoClient(DB_URI);

    try {
        await client.connect();
        console.log('Connected to MongoDB');

        return client.db(DB_NAME);
    } catch (error: any) {
        console.error('Error connecting to MongoDB:', error);
        throw error;
    }
}

async function updateSyncState(stateCollection: Collection<any>, timestamp: Date) {
    // we want to make sure that any of our sync operations are timestamped in state, so we don't duplicate anonimisation
    // we check if the sync is running along and there are more documents to sync, then we don't need to update sync state
    // otherwise we break the sync, as other customers were created and more recent createdAt is stored in sync state
    // otherwise, we update sync state as we performed sync operations
    const newTimestamp: Date = new Date();
    const checkNewState: any = await stateCollection.findOne({});

    const currentTimestampState: Date = checkNewState ? new Date(checkNewState.state) : new Date('1970-01-01');

    if (currentTimestampState.getTime() === timestamp.getTime()) {
        await persistLastInsertedTimestamp(newTimestamp, stateCollection);
    }
}

async function persistLastInsertedTimestamp(
    lastInsertedCustomerCreatedAt: Date,
    stateCollection: Collection<any>
): Promise<void> {
    await stateCollection.deleteMany({});
    const state = {state: lastInsertedCustomerCreatedAt};

    await stateCollection.insertOne(state);
}


export {
    connectToMongo,
    updateSyncState,
    persistLastInsertedTimestamp,
    DB_COLLECTION_CUSTOMERS,
    DB_COLLECTION_ANONYMIZED,
    DB_COLLECTION_STATE
};