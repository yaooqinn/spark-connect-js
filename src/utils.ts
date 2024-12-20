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