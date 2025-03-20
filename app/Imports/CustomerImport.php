<?php

namespace App\Imports;

use App\Enums\MaritalStatusEnum;
use App\Models\Customer;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Support\Collection;
use Maatwebsite\Excel\Concerns\ToCollection;
use Maatwebsite\Excel\Concerns\WithBatchInserts;
use Maatwebsite\Excel\Concerns\WithChunkReading;
use Maatwebsite\Excel\Concerns\WithHeadingRow;

class CustomerImport implements ToCollection, WithChunkReading, WithHeadingRow, ShouldQueue
{
    /**
    * @param Collection $collection
    */
    public function collection(Collection $collection)
    {
        $now = now()->format('Y-m-d H:i:s');
        $customersData = $collection->map(fn ($row) => [
            'created_at' => $now,
            'updated_at' => $now
        ])->all();

        Customer::insert($customersData);
        $customerIds = Customer::latest()->take(count($customersData))->pluck('id')->toArray();

        // Step 2: Prepare and insert properties
        $propertiesData = $collection->values()->map(function ($row, $index) use ($customerIds, $now) {
            return [
                'customer_id' => $customerIds[$index],
                'unit' => $row['unit'],
                'block' => $row['block'],
                'buy_value' => $row['buy_value'],
                'outstanding_balance' => $row['outstanding_balance'],
                'created_at' => $now,
                'updated_at' => $now,
            ];
        })->all();
        \DB::table('properties')->insert($propertiesData);

        // Step 3: Prepare and insert holders (people)
        $peopleData = $collection->values()->map(function ($row, $index) use ($customerIds, $now) {
            return [
                'customer_id' => $customerIds[$index],
                'name' => $row['name'],
                'phone' => $row['phone'],
                'email' => $row['email'],
                'cpf' => $row['cpf'],
                'rg' => $row['rg'],
                'rg_emitter' => $row['rg_emitter'],
                'rg_issue_date' => $row['rg_issue_date'],
                'nationality' => $row['nationality'],
                'naturalness' => $row['naturalness'],
                'mother_name' => $row['mother_name'],
                'father_name' => $row['father_name'],
                'birthdate' => $row['birthdate'],
                'marital_status' => $row['marital_status'],
                'created_at' => $now,
                'updated_at' => $now,
            ];
        })->all();
        \DB::table('people')->insert($peopleData);
        $peopleIds = \DB::table('people')->latest()->take(count($peopleData))->pluck('id')->toArray();

        foreach ($peopleData as $index => &$data) {
            $data['person_id'] = $peopleIds[$index];
        }

        unset($data);

        // Step 4: Prepare and insert addresses
        $addressesData = $collection->values()->map(function ($row, $index) use ($peopleIds, $now) {
            return [
                'person_id' => $peopleIds[$index],
                'address' => $row['address'],
                'number' => $row['number'],
                'complement' => $row['complement'],
                'zipcode' => $row['zipcode'],
                'district' => $row['district'],
                'city' => $row['city'],
                'state' => $row['state'],
                'created_at' => $now,
                'updated_at' => $now,
            ];
        })->all();
        \DB::table('addresses')->insert($addressesData);

        // Step 5: Prepare and insert spouses when married
        $spousesData = $collection
            ->filter(fn ($row) => strtolower($row['marital_status']) === strtolower(MaritalStatusEnum::Married->value))
            ->values()
            ->map(function ($row, $index) use ($customerIds, $now, $peopleData) {

                $peopleData = array_find($peopleData, function($value) use ($row) {
                    return $value['cpf'] === $row['cpf'];
                });

                return [
                    'customer_id' => $peopleData['customer_id'],
                    'person_id' => $peopleData['person_id'],
                    'name' => $row['c_name'],
                    'phone' => $row['c_phone'],
                    'email' => $row['c_email'],
                    'cpf' => $row['c_cpf'],
                    'rg' => $row['c_rg'],
                    'rg_emitter' => $row['c_rg_emitter'],
                    'rg_issue_date' => $row['c_rg_issue_date'],
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            })->all();

        if (!empty($spousesData)) {
            \DB::table('people')->insert($spousesData);
        }
    }

    public function chunkSize(): int
    {
        return 1000;
    }

}
