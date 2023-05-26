import {MongoClient, Db, Collection, InsertManyResult} from 'mongodb';
import { faker } from '@faker-js/faker';
import dotenv from 'dotenv';

dotenv.config();

const DB_URI: string = process.env.DB_URI || '';
const DB_NAME: string = 'ecommerce-store';
const DB_COLLECTION_CUSTOMERS: string = 'customers';
const INTERVAL_MILLISECONDS: number = 200;

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

interface Address {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
}

interface Customer {
    firstName: string;
    lastName: string;
    email: string;
    address: Address;
    createdAt: Date;
}

function generateRandomCustomer(): Customer {
    const firstName: string = faker.person.firstName();
    const lastName: string = faker.person.lastName();
    const email: string = faker.internet.email();
    const address: Address = {
        line1: faker.location.streetAddress(),
        line2: faker.location.secondaryAddress(),
        postcode: faker.location.zipCode(),
        city: faker.location.city(),
        state: faker.location.state(),
        country: faker.location.country(),
    };
    const createdAt: Date = new Date();

    return {
        firstName,
        lastName,
        email,
        address,
        createdAt,
    };
}

async function insertCustomerBatch(db: Db): Promise<void> {
    const collection: Collection<Customer> = db.collection(DB_COLLECTION_CUSTOMERS);
    const batchSize: number = Math.floor(Math.random() * 10) + 1;

    const customers: Customer[] = Array.from({ length: batchSize }, generateRandomCustomer);

    const result: InsertManyResult<Customer> = await collection.insertMany(customers);

    console.log(`Inserted ${result.insertedCount} customers`);
}

async function startGenerating(): Promise<void> {
    const db: Db = await connectToMongo();
    setInterval(async () => {
        await insertCustomerBatch(db);
    }, INTERVAL_MILLISECONDS);
}

startGenerating().catch((error: any) => {
    console.error('Error running sync script:', error);
    process.exit(1);
});
