
import {  Entity, Column, PrimaryColumn } from "typeorm";

@Entity()
export class KV {
    @PrimaryColumn()
    id: string = "t"

    @Column()
    value: string = "v"
}