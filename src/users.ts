import { Role } from './roles';

export type User = {
    name: string;
    role: Role;
    id?: string; // Filled after registration
};
