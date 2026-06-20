from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
from config.settings import settings


class PatientDataGenerator:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            api_version=(2, 5, 0)
        )

        self.hospitals = ["Hospital_A", "Hospital_B", "Hospital_C"]
        self.record_count = 0
        self.error_count = 0

    def close(self):
        self.producer.flush()
        self.producer.close()

    def generate_patient_record(self, hospital_id, patient_num, inject_error=False):

        if not inject_error:
            return {
                "patient_id": f"{hospital_id}_PAT_{patient_num:05d}",
                "hospital": hospital_id,
                "age": random.randint(18, 95),
                "bp_systolic": random.randint(100, 160),
                "bp_diastolic": random.randint(60, 100),
                "temperature": round(36.5 + random.uniform(-1, 1), 1),
                "heart_rate": random.randint(60, 100),
                "timestamp": datetime.now().isoformat()
            }

        else:
            error_type = random.choice([
                'negative_age',
                'extreme_bp',
                'invalid_temp',
                'missing_field',
                'duplicate'
            ])

            if error_type == 'negative_age':
                return {
                    "patient_id": f"{hospital_id}_PAT_{patient_num:05d}",
                    "hospital": hospital_id,
                    "age": -5,
                    "bp_systolic": random.randint(100, 160),
                    "temperature": round(36.5 + random.uniform(-1, 1), 1),
                    "timestamp": datetime.now().isoformat()
                }

            elif error_type == 'extreme_bp':
                return {
                    "patient_id": f"{hospital_id}_PAT_{patient_num:05d}",
                    "hospital": hospital_id,
                    "age": random.randint(18, 95),
                    "bp_systolic": 300,
                    "temperature": round(36.5 + random.uniform(-1, 1), 1),
                    "timestamp": datetime.now().isoformat()
                }

            elif error_type == 'invalid_temp':
                return {
                    "patient_id": f"{hospital_id}_PAT_{patient_num:05d}",
                    "hospital": hospital_id,
                    "age": random.randint(18, 95),
                    "bp_systolic": random.randint(100, 160),
                    "temperature": 50.0,
                    "timestamp": datetime.now().isoformat()
                }

            elif error_type == 'missing_field':
                record = {
                    "patient_id": f"{hospital_id}_PAT_{patient_num:05d}",
                    "hospital": hospital_id,
                    "age": random.randint(18, 95),
                    "bp_systolic": random.randint(100, 160),
                    "timestamp": datetime.now().isoformat()
                }

                return record

            else:
                return {
                    "patient_id": f"{hospital_id}_PAT_DUPLICATE_{patient_num:05d}",
                    "hospital": hospital_id,
                    "age": random.randint(18, 95),
                    "bp_systolic": random.randint(100, 160),
                    "temperature": round(36.5 + random.uniform(-1, 1), 1),
                    "timestamp": datetime.now().isoformat()
                }

    def run(self, num_records=100, error_rate=0.1):

        print("=" * 80)
        print(f"DataGuardian: Patient Data Generator")
        print(f"Kafka: {settings.KAFKA_BROKERS}")
        print(f"Topic: {settings.KAFKA_RAW_TOPIC}")
        print(f"Records: {num_records}")
        print(f"Error Rate: {error_rate*100}%")
        print("=" * 80)
        print()

        try:
            for i in range(num_records):
                hospital = random.choice(self.hospitals)
                has_error = random.random() < error_rate
                record = self.generate_patient_record(
                    hospital_id=hospital,
                    patient_num=i,
                    inject_error=has_error
                )
                self.producer.send(settings.KAFKA_RAW_TOPIC, record)

                self.record_count += 1
                if has_error:
                    self.error_count += 1

                status = "ERROR" if has_error else "OK"
                print(f"[{i+1}/{num_records}] {status} | {hospital} | "
                      f"{record['patient_id']} | Age: {record.get('age', 'NULL')} | "
                      f"BP: {record.get('bp_systolic', 'NULL')}")
                time.sleep(0.1)
            self.producer.flush()

            print()
            print("=" * 80)
            print(f" Generation complete!")
            print(f"  Total records: {self.record_count}")
            print(
                f"  With errors: {self.error_count} ({self.error_count/self.record_count*100:.1f}%)")
            print(f"  Good records: {self.record_count - self.error_count}")
            print("=" * 80)

        except Exception as e:
            print(f"Error: {e}")
        # finally:
        #     self.producer.close()


if __name__ == "__main__":
    generator = PatientDataGenerator()

    generator.run(num_records=50, error_rate=0.1)

    print("\nContinuous generation (Ctrl+C to stop)...")
    try:
        while True:
            generator.run(num_records=10, error_rate=0.1)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        generator.close()
