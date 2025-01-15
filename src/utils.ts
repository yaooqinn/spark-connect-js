export class PlanIdGenerator {
  private static instance: PlanIdGenerator;
  private currentId: bigint = 0n;
  private constructor() {}

  public static getInstance(): PlanIdGenerator {
    if (!PlanIdGenerator.instance) {
      PlanIdGenerator.instance = new PlanIdGenerator();
    }
    return PlanIdGenerator.instance;
  }

  public getNextId(): bigint {
    return this.currentId++;
  }
}

export function bigIntToNumber(value: bigint): number {
  if (value > Number.MAX_SAFE_INTEGER || value < Number.MIN_SAFE_INTEGER) {
    throw new TypeError(`Cannot convert bigint ${value} to number without losing precision`);
  }
  return Number(value);
}