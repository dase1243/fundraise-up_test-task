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

export {
    Customer,
    Address,
}