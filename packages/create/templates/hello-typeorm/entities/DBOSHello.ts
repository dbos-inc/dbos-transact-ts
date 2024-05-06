import { Entity, PrimaryGeneratedColumn, Column } from "typeorm";

@Entity()
export class DBOSHello {
    @PrimaryGeneratedColumn()
    greeting_id: number = 0;

    @Column()
    greeting: string = "greeting";
}
