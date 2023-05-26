import {MongoClient, Db, Collection, InsertManyResult} from 'mongodb';
import { faker } from '@faker-js/faker';
import dotenv from 'dotenv';
import {Address, Customer} from "./interfaces";
import {connectToMongo, DB_COLLECTION_CUSTOMERS} from "./database";

const INTERVAL_MILLISECONDS: number = 200;

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
    const batchSize: number = Math.floor(Math.random() * 10) + 1;

    const collection: Collection<Customer> = db.collection(DB_COLLECTION_CUSTOMERS);

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
