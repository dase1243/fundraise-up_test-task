import {MongoClient, Db, Collection} from 'mongodb';
import {faker} from '@faker-js/faker';
import dotenv from 'dotenv';

dotenv.config();

const DB_URI = process.env.DB_URI || "";

const DB_NAME = 'ecommerce-store';

const DB_COLLECTION_CUSTOMERS = 'customers';

const intervalMilliseconds = 200;

async function connectToMongo(): Promise<Db> {
    const client = new MongoClient(DB_URI);

    try {
        await client.connect();
        console.log('Connected to MongoDB');

        const db = client.db(DB_NAME);
        return db;
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        throw error;
    }
}

function generateRandomCustomer(): any {
    const firstName = faker.person.firstName();
    const lastName = faker.person.lastName();
    const email = faker.internet.email();
    const address = {
        line1: faker.location.streetAddress(),
        line2: faker.location.secondaryAddress(),
        postcode: faker.location.zipCode(),
        city: faker.location.city(),
        state: faker.location.state(),
        country: faker.location.country(),
    };
    const createdAt = new Date();

    return {
        firstName,
        lastName,
        email,
        address,
        createdAt,
    };
}

async function insertCustomerBatch(db: Db): Promise<void> {
    const collection = db.collection(DB_COLLECTION_CUSTOMERS);
    const batchSize: number = Math.floor(Math.random() * 10) + 1;

    const customers = Array.from({length: batchSize}, () => generateRandomCustomer());

    await collection.insertMany(customers);

    console.log(`Inserted ${batchSize} customers`);
}

async function startGenerating(): Promise<void> {
    const db = await connectToMongo();
    setInterval(async () => {
        await insertCustomerBatch(db);
    }, intervalMilliseconds);
}

startGenerating().catch((error) => {
    console.error('Error running sync script:', error);
    process.exit(1);
});