import {
    ChangeStream,
    Collection,
    InsertOneResult,
    Db, WithId, FindCursor,
} from 'mongodb';
import crypto from 'crypto';
import {Customer} from "./interfaces";
import {
    connectToMongo,
    DB_COLLECTION_ANONYMIZED,
    DB_COLLECTION_CUSTOMERS,
    DB_COLLECTION_STATE, persistLastInsertedTimestamp,
    updateSyncState
} from "./database";

const FULL_REINDEX_FLAG: string = '--full-reindex';

const intervalMilliseconds: number = 1000;

function anonymizeValue(value: string): string {
    const hash: string = crypto.createHash('sha1').update(value).digest('hex');
    return hash.slice(0, 8);
}

function anonymizeCustomer(customer: Customer): Customer {
    const tmpCustomer: Customer = {...customer};
    const anonymizedCustomer: Customer = {
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
    };

    anonymizedCustomer.firstName = anonymizeValue(tmpCustomer.firstName);
    anonymizedCustomer.lastName = anonymizeValue(tmpCustomer.lastName);

    const splitEmail: string[] = tmpCustomer.email.split('@');
    anonymizedCustomer.email = anonymizeValue(splitEmail[0]) + '@' + splitEmail[1];

    anonymizedCustomer.address.line1 = anonymizeValue(tmpCustomer.address.line1);
    anonymizedCustomer.address.line2 = anonymizeValue(tmpCustomer.address.line2);
    anonymizedCustomer.address.postcode = anonymizeValue(tmpCustomer.address.postcode);

    return anonymizedCustomer;
}


async function anonymizeCursorCustomers(cursor: FindCursor<WithId<Customer>>, anonymizedCollection: Collection<Customer>) {
    let anonymizedCustomer: Customer;
    let insertedResult: InsertOneResult<Customer>;
    while (await cursor.hasNext()) {
        anonymizedCustomer = anonymizeCustomer(await cursor.next() as Customer);
        insertedResult = await anonymizedCollection.insertOne(anonymizedCustomer);
        console.log(`Anonymized customer is inserted: ${insertedResult.insertedId}`);
    }
}

async function findAndAnonymize(timestamp: Date, customersCollection: Collection<Customer>, anonymizedCollection: Collection<Customer>, stateCollection: Collection<any>) {
    // we filter customers between 1970-01-01 and now
    let filter = {
        createdAt: {
            $gte: timestamp,
            $lte: new Date(),
        },
    };
    const cursor = customersCollection.find(filter);

    // going through each customer and anonymising it
    await anonymizeCursorCustomers(cursor, anonymizedCollection);

    await cursor.close();

    await updateSyncState(stateCollection, timestamp);
}

async function runSyncScript(db: Db): Promise<void> {
    const batchSize: number = 1000;
    let batch: Customer[] = [];

    const customersCollection: Collection<Customer> = await db.collection(DB_COLLECTION_CUSTOMERS);
    const anonymizedCollection: Collection<Customer> = await db.collection(DB_COLLECTION_ANONYMIZED);
    const stateCollection: Collection<any> = await db.collection(DB_COLLECTION_STATE);

    console.log('Collection connections are opened');

    // if full reindex flag is used
    if (process.argv.includes(FULL_REINDEX_FLAG)) {
        console.log('Starting full reindex');

        const timestamp: Date = new Date('1970-01-01');

        // Clear customers_anonymised collection
        await anonymizedCollection.deleteMany({});

        // re-anonimyse customers
        await findAndAnonymize(timestamp, customersCollection, anonymizedCollection, stateCollection);

        process.exit(0); // Exit after full reindex
    } else {
        console.log('Starting sync script');

        const syncWhileOfflineDocuments = async (): Promise<void> => {
            console.log('Anonymize offline inserted documents');

            const lastState: any = await stateCollection.findOne({});

            const timestamp: Date = lastState ? new Date(lastState.state) : new Date('1970-01-01');

            await findAndAnonymize(timestamp, customersCollection, anonymizedCollection, stateCollection);
        };

        syncWhileOfflineDocuments()
            .then(() => {
                console.log('Finished anonymization of offline inserted documents');
            })
            .catch((error) => {
                console.error('Error running the function of anonymization of offline inserted documents:', error);
            });

        const processBatch = async (): Promise<void> => {
            if (batch.length > 0) {
                console.log('Sync the collected batch');

                const lastInsertedCustomerCreatedAt: Date = batch[batch.length - 1].createdAt;

                // console.log(`lastInsertedCustomerCreatedAt: ${JSON.stringify(offlineInsertedCustomers)}`);

                const anonymizedBatch: Customer[] = batch.map((customer) => anonymizeCustomer(customer));

                await anonymizedCollection.insertMany(anonymizedBatch);

                await persistLastInsertedTimestamp(new Date(lastInsertedCustomerCreatedAt), stateCollection);

                console.log(`Inserted ${batch.length} anonymized customers`);

                batch = [];
            }
        };

        const changeStream: ChangeStream = customersCollection.watch();

        changeStream.on('change', async (change) => {
            if (change.operationType === 'insert' || change.operationType === 'update') {
                batch.push(change.fullDocument as Customer);
            }

            if (batch.length >= batchSize) {
                await processBatch();
            }
        });

        setInterval(processBatch, intervalMilliseconds);
    }
}

async function startSync(): Promise<void> {
    const db: Db = await connectToMongo();

    runSyncScript(db)
        .finally(() => {
            console.log('Sync script is closed');
        });
}

startSync().catch((error: any) => {
    console.error('Error running sync script:', error);
    process.exit(1);
});