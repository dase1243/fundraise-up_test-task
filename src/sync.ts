import {
    MongoClient,
    ChangeStream,
    ObjectId,
    Collection,
    InsertOneResult,
} from 'mongodb';
import crypto from 'crypto';
import dotenv from 'dotenv';

dotenv.config();

const DB_URI = process.env.DB_URI || "";

const DB_NAME = 'ecommerce-store';

const DB_COLLECTION = 'customers';
const DB_COLLECTION_ANONYMIZED = 'customers_anonymised';
const DB_COLLECTION_STATE = 'customers_anonymization_state';

const FULL_REINDEX_FLAG = '--full-reindex';

const intervalMilliseconds = 1000;

async function connectToMongo(): Promise<MongoClient> {
    const client = new MongoClient(DB_URI);

    try {
        await client.connect();
        console.log('Connected to MongoDB');

        return client;
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        throw error;
    }
}

function anonymizeValue(value: string): string {
    const hash = crypto.createHash('sha1').update(value).digest('hex');
    return hash.slice(0, 8);
}

function anonymizeCustomer(customer: any) {
    const tmpCustomer = {...customer};
    const anonymizedCustomer = {
        firstName: '',
        lastName: '',
        email: '',
        address: {
            line1: '',
            line2: '',
            postcode: '',
            city: '',
            state: '',
            country: '',
        },
        createdAt: new Date(),
    }

    anonymizedCustomer.firstName = anonymizeValue(tmpCustomer.firstName);
    anonymizedCustomer.lastName = anonymizeValue(tmpCustomer.lastName);

    let splitEmail = tmpCustomer.email.split('@');
    anonymizedCustomer.email = anonymizeValue(splitEmail[0]) + '@' + splitEmail[1];

    anonymizedCustomer.address.line1 = anonymizeValue(tmpCustomer.address.line1);
    anonymizedCustomer.address.line2 = anonymizeValue(tmpCustomer.address.line2);
    anonymizedCustomer.address.postcode = anonymizeValue(tmpCustomer.address.postcode);

    return anonymizedCustomer;
}

async function persistLastInsertedTimestamp(lastInsertedCustomerCreatedAt: Date, stateCollection: Collection<any>): Promise<void> {
    await stateCollection.deleteMany({});
    const state = {state: lastInsertedCustomerCreatedAt};

    await stateCollection.insertOne(state);
}


async function runSyncScript(mongoClient: MongoClient): Promise<void> {
    const batchSize = 1000;
    let batch: any[] = [];

    const db = await mongoClient.db(DB_NAME);
    const customersCollection = await db.collection(DB_COLLECTION);
    const anonymizedCollection = await db.collection(DB_COLLECTION_ANONYMIZED);
    const stateCollection = await db.collection(DB_COLLECTION_STATE);

    console.log('Collection connections are opened');

    if (process.argv.includes(FULL_REINDEX_FLAG)) {
        console.log('Starting full reindex');

        await anonymizedCollection.deleteMany({}); // Clear target collection

        const cursor = await customersCollection.find({
            createdAt: {
                $lte: new Date()
            }
        });

        let anonymizedCustomer;
        let insertedResult: InsertOneResult;
        while (await cursor.hasNext()) {
            anonymizedCustomer = anonymizeCustomer(await cursor.next());
            insertedResult = await anonymizedCollection.insertOne(anonymizedCustomer);
            // console.log(`Anonimysed customer is inserted: ${insertedResult.insertedId}`);
        }

        await cursor.close();

        process.exit(0); // Exit after full reindex
    } else {
        console.log('Starting sync script');

        const syncWhileOfflineDocuments = async () => {
            console.log('Anonymize offline inserted documents');

            const lastState = await stateCollection.findOne({});

            const timestamp = lastState ? new Date(lastState.state) : new Date('1970-01-01');

            console.log(`timestamp: ${timestamp}`);

            const offlineInsertedCustomersCursor = await customersCollection.find({
                createdAt: {
                    $gte: timestamp,
                    $lte: new Date()
                }
            });

            let anonymizedCustomer;
            let insertedResult;
            while (await offlineInsertedCustomersCursor.hasNext()) {
                anonymizedCustomer = anonymizeCustomer(await offlineInsertedCustomersCursor.next());
                insertedResult = await anonymizedCollection.insertOne(anonymizedCustomer);
                console.log(`Anonimysed customer is inserted: ${insertedResult.insertedId}`);
            }

            // check if the sync is running along and there are more documents to sync
            // if yes, we don't need to update the state, otherwise we have to
            const newTimestamp = new Date();
            const checkNewState = await stateCollection.findOne({});

            const currentTimestampState = checkNewState ? new Date(checkNewState.state) : new Date();

            if (currentTimestampState.getTime() === timestamp.getTime()) {
                await persistLastInsertedTimestamp(newTimestamp, stateCollection);
            }
        };

        syncWhileOfflineDocuments()
            .then(() => {
                console.log('Finished anonymization of offline inserted documents');
            })
            .catch((error) => {
                console.error('Error running the function of anonymization of offline inserted documents:', error);
            });


        const processBatch = async () => {
            if (batch.length > 0) {
                console.log('Sync the collected batch');

                const lastInsertedCustomerCreatedAt = batch[batch.length - 1].createdAt;

                // console.log(`lastInsertedCustomerCreatedAt: ${JSON.stringify(offlineInsertedCustomers)}`);

                const anonymizedBatch = batch.map((customer) => anonymizeCustomer(customer));

                await anonymizedCollection.insertMany(anonymizedBatch);

                await persistLastInsertedTimestamp(new Date(lastInsertedCustomerCreatedAt), stateCollection);

                console.log(`Inserted ${batch.length} anonymized customers`);

                batch = [];
            }
        };

        const changeStream: ChangeStream = customersCollection.watch();

        changeStream.on('change', async (change) => {
            if (change.operationType === 'insert' || change.operationType === 'update') {
                batch.push(change.fullDocument);
            }

            if (batch.length >= batchSize) {
                await processBatch();
            }
        });

        setInterval(processBatch, intervalMilliseconds);
    }
}


async function startSync(): Promise<void> {
    const mongoClient = await connectToMongo();

    runSyncScript(mongoClient)
        .finally(() => {
            console.log('Sync script is closed');
        })
}

startSync().catch((error) => {
    console.error('Error running sync script:', error);
    process.exit(1);
})