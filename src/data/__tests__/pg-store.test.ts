import { Entity } from "of-lib-domain";
import { PgDbContext } from "../pg-context";
import { PgDbStore } from "../pg-store";

class Test extends Entity<string> {
  constructor(id, public name: string) {
    super(id);
  }
}

class TestStore extends PgDbStore<Test, string> {}

const url = "postgresql://postgres:abc123@192.168.1.101:5432/auth";
const context: PgDbContext = new PgDbContext(url);

beforeAll(async () => {
  const pool = await context.getPool();

  await pool.query(`DELETE FROM test.test_clean`);
  await pool.query(`DELETE FROM test.test_delete`);
  await pool.query(`DELETE FROM test.test_exists`);
  await pool.query(`DELETE FROM test.test_find`);
  await pool.query(`DELETE FROM test.test_find_one`);
  await pool.query(`DELETE FROM test.test_insert`);
  return await pool.query(`DELETE FROM test.test_replace`);
});

afterAll(async () => {
  return await context.close(true);
});

test("connection", async () => {
  const client = await context.getClient();

  try {
    expect(client).toBeTruthy();
  } finally {
    client.release();
  }
});

test("insert", async () => {
  const client = await context.getClient();

  try {
    const tableName = "test.test_insert";
    const store = new TestStore(client, tableName, ["id", "name"], "id");

    const id = Math.round(Math.random() * 10000).toString();
    const r = await store.create(new Test(id, "Diego"));

    expect(r).toBe(id);
  } finally {
    client.release();
  }
});

test("delete", async () => {
  const client = await context.getClient();

  try {
    const tableName = "test.test_delete";
    const store = new TestStore(client, tableName, ["id", "name"], "id");

    const id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Diego"));

    const r = await store.delete(id);

    expect(r).toBe(true);
  } finally {
    client.release();
  }
});

test("replace", async () => {
  const client = await context.getClient();

  try {
    const tableName = "test.test_replace";
    const store = new TestStore(client, tableName, ["id", "name"], "id");

    const id = Math.round(Math.random() * 10000).toString();
    const t = new Test(id, "Diego");
    await store.create(t);

    t.name = "Paco";
    const r = await store.replace(id, t);

    expect(r).toBe(true);
  } finally {
    client.release();
  }
});

test("exists", async () => {
  const client = await context.getClient();

  try {
    const tableName = "test.test_exists";
    const store = new TestStore(client, tableName, ["id", "name"], "id");

    const id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Diego"));

    const r = await store.exists({ name: "Diego" });

    expect(r).toBe(true);
  } finally {
    client.release();
  }
});

test("findOne", async () => {
  const client = await context.getClient();

  try {
    const tableName = "test.test_find_one";
    const store = new TestStore(client, tableName, ["id", "name"], "id");

    const id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Diego"));

    const r = (await store.findOne(id)) as any;

    expect(r.id).toBe(id);
    expect(r.name).toBe("Diego");
  } finally {
    client.release();
  }
});

test("find", async () => {
  const client = await context.getClient();

  try {
    const tableName = "test.test_find";
    const store = new TestStore(client, tableName, ["id", "name"], "id");

    let id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Diego"));

    id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Juan"));

    id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Pablo"));

    id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "María"));

    id = Math.round(Math.random() * 10000).toString();
    await store.create(new Test(id, "Juana"));

    let r = await store.find({});

    expect(r.count).toBe(5);
    expect(r.items.length).toBe(5);

    r = await store.find({ name: "Diego" }, 1, 3);

    expect(r.count).toBe(1);
    expect(r.items.length).toBe(1);

    r = await store.find({}, 2, 3);

    expect(r.count).toBe(5);
    expect(r.items.length).toBe(2);
  } finally {
    client.release();
  }
});
