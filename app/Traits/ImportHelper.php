<?php

namespace App\Traits;

use App\Enums\MaritalStatusEnum;
use App\Models\Address;
use App\Models\Customer;
use App\Models\Person;
use App\Models\Property;
use Carbon\Carbon;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Concurrency;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\LazyCollection;
use Illuminate\Support\Str;
use PDO;
use PDOStatement;

use function Laravel\Prompts\select;

trait ImportHelper
{
    protected float $benchmarkStartTime;

    protected int $benchmarkStartMemory;

    protected int $startRowCount;

    protected int $startQueries;

    public function handle(): void
    {
        error_reporting(E_ALL);
        ini_set('display_errors', 1);
        $filePath = $this->selectFile();
        $this->startBenchmark();

        try {
            $this->handleImport($filePath);
        } catch (\Exception $e) {
            $this->error(get_class($e).' '.Str::of($e->getMessage())->limit(100)->value());
        }

        $this->endBenchmark();
    }

    protected function selectFile(): string
    {
        $file = select(
            label: 'What file do you want to import?',
            options: ['CSV 100 Clientes', 'CSV 1K Clientes', 'CSV 10K Clientes', 'CSV 100K Clientes', 'CSV 1M Clientes', 'CSV 10M Clientes']
        );

        return match ($file) {
            'CSV 100 Clientes' => base_path('customers-100.csv'),
            'CSV 1K Clientes' => base_path('customers-1000.csv'),
            'CSV 10K Clientes' => base_path('customers-10000.csv'),
            'CSV 100K Clientes' => base_path('customers-100000.csv'),
            'CSV 1M Clientes' => base_path('customers-1000000.csv'),
            'CSV 10M Clientes' => base_path('customers-10000000.csv'),
        };
    }

    protected function startBenchmark(string $table = 'customers'): void
    {
        $this->startRowCount = DB::table($table)->count()
            + DB::table('properties')->count()
            + DB::table('people')->count()
            + DB::table('addresses')->count();
        $this->benchmarkStartTime = microtime(true);
        $this->benchmarkStartMemory = memory_get_usage();
        DB::enableQueryLog();
        $this->startQueries = DB::select("SHOW SESSION STATUS LIKE 'Questions'")[0]->Value;
    }

    protected function endBenchmark(string $table = 'customers'): void
    {
        $executionTime = microtime(true) - $this->benchmarkStartTime;
        $memoryUsage = round((memory_get_usage() - $this->benchmarkStartMemory) / 1024 / 1024, 2);
        $queriesCount = DB::select("SHOW SESSION STATUS LIKE 'Questions'")[0]->Value - $this->startQueries - 1; // Subtract the Questions query itself

        // Get row count after we've stopped tracking queries
        $rowDiff = (DB::table($table)->count()
            + DB::table('properties')->count()
            + DB::table('people')->count()
        + DB::table('addresses')->count()) - $this->startRowCount;

        $formattedTime = match (true) {
            $executionTime >= 60 => sprintf('%dm %ds', floor($executionTime / 60), $executionTime % 60),
            $executionTime >= 1 => round($executionTime, 2).'s',
            default => round($executionTime * 1000).'ms',
        };

        $this->newLine();
        $this->line(sprintf(
            '⚡ <bg=bright-blue;fg=black> TIME: %s </> <bg=bright-green;fg=black> MEM: %sMB </> <bg=bright-yellow;fg=black> SQL: %s </> <bg=bright-magenta;fg=black> ROWS: %s </>',
            $formattedTime,
            $memoryUsage,
            number_format($queriesCount),
            number_format($rowDiff)
        ));
        $this->newLine();
    }

    private function import01BasicOneByOne(string $filePath): void
    {
        collect(file($filePath))
            ->skip(1)
            ->map(fn ($line) => str_getcsv($line))
            ->map(fn ($row) => [
                'unit' => $row[0],
                'block' => $row[1],
                'buy_value' => $row[2],
                'outstanding_balance' => $row[3],
                'name' => $row[4],
                'phone' => $row[5],
                'email' => $row[6],
                'cpf' => $row[7],
                'rg' => $row[8],
                'rg_emitter' => $row[9],
                'rg_issue_date' => Carbon::createFromFormat('d/m/Y', $row[10])->format('Y-m-d'),
                'address' => $row[11],
                'number' => $row[12],
                'complement' => $row[13],
                'zipcode' => $row[14],
                'district' => $row[15],
                'city' => $row[16],
                'state' => $row[17],
                'nationality' => $row[18],
                'naturalness' => $row[19],
                'mother_name' => $row[20],
                'father_name' => $row[21],
                'birthdate' => Carbon::createFromFormat('d/m/Y', $row[22])->format('Y-m-d'),
                'marital_status' => $row[23],
                'c_name' => $row[24],
                'c_phone' => $row[25],
                'c_email' => $row[26],
                'c_cpf' => $row[27],
                'c_rg' => $row[28],
                'c_rg_emitter' => $row[29],
                'c_rg_issue_date' => Carbon::createFromFormat('d/m/Y', $row[30])->format('Y-m-d'),
            ])
            ->each(function ($data) {
                $customer = Customer::create([]);

                $customer->properties()->create($data);

                $holder = $customer->people()->create($data);

                $holder->addresses()->create($data);

                if (strtolower($data['marital_status']) === strtolower(MaritalStatusEnum::Married->value)) {
                    $spouse = $customer->people()->create([
                        'person_id' => $holder->id,
                        'name' => $data['c_name'],
                        'phone' => $data['c_phone'],
                        'email' => $data['c_email'],
                        'cpf' => $data['c_cpf'],
                        'rg' => $data['c_rg'],
                        'rg_emitter' => $data['c_rg_emitter'],
                        'rg_issue_date' => $data['c_rg_issue_date'],
                    ]);
                }

            });
    }

    private function import02CollectAndInsert(string $filePath): void
    {
        $now = now()->format('Y-m-d H:i:s');

        $data = collect(file($filePath))
            ->skip(1) // Skip header row
            ->map(fn ($line) => str_getcsv($line))
            ->map(function ($row) use ($now) {
                return [
                    'unit' => $row[0],
                    'block' => $row[1],
                    'buy_value' => $row[2],
                    'outstanding_balance' => $row[3],
                    'name' => $row[4],
                    'phone' => $row[5],
                    'email' => $row[6],
                    'cpf' => $row[7],
                    'rg' => $row[8],
                    'rg_emitter' => $row[9],
                    'rg_issue_date' => Carbon::createFromFormat('d/m/Y', $row[10])->format('Y-m-d'),
                    'address' => $row[11],
                    'number' => $row[12],
                    'complement' => $row[13],
                    'zipcode' => $row[14],
                    'district' => $row[15],
                    'city' => $row[16],
                    'state' => $row[17],
                    'nationality' => $row[18],
                    'naturalness' => $row[19],
                    'mother_name' => $row[20],
                    'father_name' => $row[21],
                    'birthdate' => Carbon::createFromFormat('d/m/Y', $row[22])->format('Y-m-d'),
                    'marital_status' => $row[23],
                    'c_name' => $row[24],
                    'c_phone' => $row[25],
                    'c_email' => $row[26],
                    'c_cpf' => $row[27],
                    'c_rg' => $row[28],
                    'c_rg_emitter' => $row[29],
                    'c_rg_issue_date' => Carbon::createFromFormat('d/m/Y', $row[30])->format('Y-m-d'),
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            });

        // Step 1: Insert all customers first
        $customersData = $data->map(fn ($row) => [
            'created_at' => $now,
            'updated_at' => $now
        ])->all();

        Customer::insert($customersData);
        $customerIds = Customer::latest()->take(count($customersData))->pluck('id')->toArray();

        // Step 2: Prepare and insert properties
        $propertiesData = $data->map(function ($row, $index) use ($customerIds, $now) {
            return [
                'customer_id' => $customerIds[$index - 1],
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
        $peopleData = collect($data)->map(function ($row, $index) use ($customerIds, $now) {
            return [
                'customer_id' => $customerIds[$index - 1],
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

        $i = 0;
        foreach ($peopleData as &$person) {
            $person['person_id'] = $peopleIds[$i];
            $i++;
        }

        // Step 4: Prepare and insert addresses
        $addressesData = collect($data)->map(function ($row, $index) use ($peopleIds, $now) {
            return [
                'person_id' => $peopleIds[$index - 1],
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
        $spousesData = $data
            ->filter(fn ($row) => strtolower($row['marital_status']) === strtolower(MaritalStatusEnum::Married->value))
            ->map(function ($row, $index) use ($customerIds, $now, $peopleIds) {

                return [
                    'customer_id' => $customerIds[$index - 1],
                    'person_id' => $peopleIds[$index - 1],
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

    private function import03CollectAndChunk(string $filePath): void
    {
        // Collect all but insert in chunks
        $now = now()->format('Y-m-d H:i:s');

        collect(file($filePath))
            ->skip(1)
            ->map(fn ($line) => str_getcsv($line))
            ->map(fn ($row) => [
                'unit' => $row[0],
                'block' => $row[1],
                'buy_value' => $row[2],
                'outstanding_balance' => $row[3],
                'name' => $row[4],
                'phone' => $row[5],
                'email' => $row[6],
                'cpf' => $row[7],
                'rg' => $row[8],
                'rg_emitter' => $row[9],
                'rg_issue_date' => Carbon::createFromFormat('d/m/Y', $row[10])->format('Y-m-d'),
                'address' => $row[11],
                'number' => $row[12],
                'complement' => $row[13],
                'zipcode' => $row[14],
                'district' => $row[15],
                'city' => $row[16],
                'state' => $row[17],
                'nationality' => $row[18],
                'naturalness' => $row[19],
                'mother_name' => $row[20],
                'father_name' => $row[21],
                'birthdate' => Carbon::createFromFormat('d/m/Y', $row[22])->format('Y-m-d'),
                'marital_status' => $row[23],
                'c_name' => $row[24],
                'c_phone' => $row[25],
                'c_email' => $row[26],
                'c_cpf' => $row[27],
                'c_rg' => $row[28],
                'c_rg_emitter' => $row[29],
                'c_rg_issue_date' => Carbon::createFromFormat('d/m/Y', $row[30])->format('Y-m-d'),
                'created_at' => $now,
                'updated_at' => $now,
            ])
            ->chunk(1000)
            ->each(function ($chunk) use ($now) {
                // Step 1: Insert all customers first
                $customersData = $chunk->map(fn ($row) => [
                    'created_at' => $now,
                    'updated_at' => $now
                ])->all();

                Customer::insert($customersData);
                $customerIds = Customer::latest()->take(count($customersData))->pluck('id')->toArray();

                // Step 2: Prepare and insert properties
                $propertiesData = $chunk->values()->map(function ($row, $index) use ($customerIds, $now) {
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
                $peopleData = $chunk->values()->map(function ($row, $index) use ($customerIds, $now) {
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
                $addressesData = $chunk->values()->map(function ($row, $index) use ($peopleIds, $now) {
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
                $spousesData = $chunk
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
            });
    }

    private function import04LazyCollection(string $filePath): void
    {
        $now = now()->format('Y-m-d H:i:s');

        LazyCollection::make(function () use ($filePath) {
            $handle = fopen($filePath, 'r');
            fgets($handle); // skip header

            while (($line = fgets($handle)) !== false) {
                yield str_getcsv($line);
            }
            fclose($handle);
        })
            ->each(function ($row) use ($now) {
                // Directly insert each row
                $customer = Customer::create([]);

                $customer->properties()->create([
                    'unit' => $row[0],
                    'block' => $row[1],
                    'buy_value' => $row[2],
                    'outstanding_balance' => $row[3],
                ]);

                $data = explode('/', $row[10]);
                $rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

                $data = explode('/', $row[22]);
                $birthdate = "{$data[2]}-{$data[1]}-{$data[0]}";

                $holder = $customer->people()->create([
                    'name' => $row[4],
                    'phone' => $row[5],
                    'email' => $row[6],
                    'cpf' => $row[7],
                    'rg' => $row[8],
                    'rg_emitter' => $row[9],
                    'rg_issue_date' => $rg_issue_date,
                    'nationality' => $row[18],
                    'naturalness' => $row[19],
                    'mother_name' => $row[20],
                    'father_name' => $row[21],
                    'birthdate' => $birthdate,
                    'marital_status' => $row[23],
                ]);

                $holder->addresses()->create([
                    'address' => $row[11],
                    'number' => $row[12],
                    'complement' => $row[13],
                    'zipcode' => $row[14],
                    'district' => $row[15],
                    'city' => $row[16],
                    'state' => $row[17],
                ]);

                if (strtolower($row[23]) === strtolower(MaritalStatusEnum::Married->value)) {

                    $data = explode('/', $row[30]);
                    $c_rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

                    $spouse = $holder->spouse()->create([
                        'customer_id' => $holder->customer_id,
                        'name' => $row[24],
                        'phone' => $row[25],
                        'email' => $row[26],
                        'cpf' => $row[27],
                        'rg' => $row[28],
                        'rg_emitter' => $row[29],
                        'rg_issue_date' => $c_rg_issue_date,
                    ]);
                }
            });
    }

    private function import05LazyCollectionWithChunking(string $filePath): void
    {

        $now = now()->format('Y-m-d H:i:s');
        $chunkSize = 1000;

        $csvRows = LazyCollection::make(function () use ($filePath) {
            $handle = fopen($filePath, 'r');
            fgets($handle); // Ignora o cabeçalho
            while (($line = fgets($handle)) !== false) {
                yield str_getcsv($line);
            }
            fclose($handle);
        })->map(function ($row) use ($now) {
            $data = explode('/', $row[10]);
            $rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

            $data = explode('/', $row[22]);
            $birthdate = "{$data[2]}-{$data[1]}-{$data[0]}";

            $data = explode('/', $row[30]);
            $c_rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

            return [
                'unit'             => $row[0],
                'block'            => $row[1],
                'buy_value'        => $row[2],
                'outstanding_balance' => $row[3],
                'name'             => $row[4],
                'phone'            => $row[5],
                'email'            => $row[6],
                'cpf'              => $row[7],
                'rg'               => $row[8],
                'rg_emitter'       => $row[9],
                'rg_issue_date'    => $rg_issue_date,
                'address'          => $row[11],
                'number'           => $row[12],
                'complement'       => $row[13],
                'zipcode'          => $row[14],
                'district'         => $row[15],
                'city'             => $row[16],
                'state'            => $row[17],
                'nationality'      => $row[18],
                'naturalness'      => $row[19],
                'mother_name'      => $row[20],
                'father_name'      => $row[21],
                'birthdate'        => $birthdate,
                'marital_status'   => $row[23],
                'c_name'           => $row[24],
                'c_phone'          => $row[25],
                'c_email'          => $row[26],
                'c_cpf'            => $row[27],
                'c_rg'             => $row[28],
                'c_rg_emitter'     => $row[29],
                'c_rg_issue_date'  => $c_rg_issue_date,
                'created_at'       => $now,
                'updated_at'       => $now,
            ];
        })
        ->chunk($chunkSize)
        ->each(function ($chunk) use ($now) {
            // Converter o chunk para array para evitar múltiplas iterações na collection
            $rows = $chunk->values()->all();

            // Etapa 1: Inserir Customers
            $customersData = [];
            foreach ($rows as $row) {
                $customersData[] = [
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            }
            Customer::insert($customersData);
            $customerIds = Customer::latest()->take(count($customersData))->pluck('id')->toArray();

            // Etapa 2: Inserir Properties
            $propertiesData = [];
            foreach ($rows as $index => $row) {
                $propertiesData[] = [
                    'customer_id'       => $customerIds[$index],
                    'unit'              => $row['unit'],
                    'block'             => $row['block'],
                    'buy_value'         => $row['buy_value'],
                    'outstanding_balance'=> $row['outstanding_balance'],
                    'created_at'        => $now,
                    'updated_at'        => $now,
                ];
            }
            \DB::table('properties')->insert($propertiesData);

            // Etapa 3: Inserir People (holders)
            $peopleData = [];
            foreach ($rows as $index => $row) {
                $peopleData[] = [
                    'customer_id'    => $customerIds[$index],
                    'name'           => $row['name'],
                    'phone'          => $row['phone'],
                    'email'          => $row['email'],
                    'cpf'            => $row['cpf'],
                    'rg'             => $row['rg'],
                    'rg_emitter'     => $row['rg_emitter'],
                    'rg_issue_date'  => $row['rg_issue_date'],
                    'nationality'    => $row['nationality'],
                    'naturalness'    => $row['naturalness'],
                    'mother_name'    => $row['mother_name'],
                    'father_name'    => $row['father_name'],
                    'birthdate'      => $row['birthdate'],
                    'marital_status' => $row['marital_status'],
                    'created_at'     => $now,
                    'updated_at'     => $now,
                ];
            }
            \DB::table('people')->insert($peopleData);
            $peopleIds = \DB::table('people')->latest()->take(count($peopleData))->pluck('id')->toArray();

            // Criar índice associativo para People usando o CPF (busca otimizada)
            $peopleIndex = [];
            foreach ($peopleData as $index => $data) {
                $peopleIndex[$data['cpf']] = [
                    'customer_id' => $customerIds[$index],
                    'person_id'   => $peopleIds[$index],
                ];
            }

            // Etapa 4: Inserir Addresses
            $addressesData = [];
            foreach ($rows as $index => $row) {
                $addressesData[] = [
                    'person_id'   => $peopleIds[$index],
                    'address'     => $row['address'],
                    'number'      => $row['number'],
                    'complement'  => $row['complement'],
                    'zipcode'     => $row['zipcode'],
                    'district'    => $row['district'],
                    'city'        => $row['city'],
                    'state'       => $row['state'],
                    'created_at'  => $now,
                    'updated_at'  => $now,
                ];
            }
            \DB::table('addresses')->insert($addressesData);

            // Etapa 5: Inserir Spouses (cônjuges) para registros com estado civil casado
            $spousesData = [];
            foreach ($rows as $row) {
                if (strtolower($row['marital_status']) === strtolower(MaritalStatusEnum::Married->value)) {
                    if (isset($peopleIndex[$row['cpf']])) {
                        $spousesData[] = [
                            'customer_id'  => $peopleIndex[$row['cpf']]['customer_id'],
                            'person_id'    => $peopleIndex[$row['cpf']]['person_id'],
                            'name'         => $row['c_name'],
                            'phone'        => $row['c_phone'],
                            'email'        => $row['c_email'],
                            'cpf'          => $row['c_cpf'],
                            'rg'           => $row['c_rg'],
                            'rg_emitter'   => $row['c_rg_emitter'],
                            'rg_issue_date'=> $row['c_rg_issue_date'],
                            'created_at'   => $now,
                            'updated_at'   => $now,
                        ];
                    }
                }
            }
            if (!empty($spousesData)) {
                \DB::table('people')->insert($spousesData);
            }
        });
    }

    private function import06LazyCollectionWithChunkingAndPdo(string $filePath): void
    {
        $now = now()->format('Y-m-d H:i:s');
        $pdo = DB::connection()->getPdo();

        LazyCollection::make(function () use ($filePath) {
            $handle = fopen($filePath, 'rb');
            // Pula o cabeçalho
            fgetcsv($handle);
            while (($line = fgetcsv($handle)) !== false) {
                yield $line;
            }
            fclose($handle);
        })
            ->chunk(1000)
            ->each(function ($chunk) use ($pdo, $now) {
                // 1. Transformação e formatação dos dados de cada linha
                $rows = [];
                foreach ($chunk as $row) {
                    // Formata as datas (rg_issue_date, birthdate e c_rg_issue_date) utilizando explode para evitar o uso do Carbon
                    $data = explode('/', $row[10]);
                    $rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

                    $data = explode('/', $row[22]);
                    $birthdate = "{$data[2]}-{$data[1]}-{$data[0]}";

                    $data = explode('/', $row[30]);
                    $c_rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

                    $rows[] = [
                        'unit'                => $row[0],
                        'block'               => $row[1],
                        'buy_value'           => $row[2],
                        'outstanding_balance' => $row[3],
                        'name'                => $row[4],
                        'phone'               => $row[5],
                        'email'               => $row[6],
                        'cpf'                 => $row[7],
                        'rg'                  => $row[8],
                        'rg_emitter'          => $row[9],
                        'rg_issue_date'       => $rg_issue_date,
                        'address'             => $row[11],
                        'number'              => $row[12],
                        'complement'          => $row[13],
                        'zipcode'             => $row[14],
                        'district'            => $row[15],
                        'city'                => $row[16],
                        'state'               => $row[17],
                        'nationality'         => $row[18],
                        'naturalness'         => $row[19],
                        'mother_name'         => $row[20],
                        'father_name'         => $row[21],
                        'birthdate'           => $birthdate,
                        'marital_status'      => $row[23],
                        'c_name'              => $row[24],
                        'c_phone'             => $row[25],
                        'c_email'             => $row[26],
                        'c_cpf'               => $row[27],
                        'c_rg'                => $row[28],
                        'c_rg_emitter'        => $row[29],
                        'c_rg_issue_date'     => $c_rg_issue_date,
                        'created_at'          => $now,
                        'updated_at'          => $now,
                    ];
                }
                $totalRows = count($rows);

                // 2. Inserir registros em customers
                $placeholders = rtrim(str_repeat('(?, ?),', $totalRows), ',');
                $sqlCustomers = 'INSERT INTO customers (created_at, updated_at) VALUES ' . $placeholders;
                $valuesCustomers = [];
                for ($i = 0; $i < $totalRows; $i++) {
                    $valuesCustomers[] = $now;
                    $valuesCustomers[] = $now;
                }
                $stmt = $pdo->prepare($sqlCustomers);
                $stmt->execute($valuesCustomers);

                // Recupera o ID do primeiro customer inserido e gera o array de IDs assumindo incremento sequencial
                $firstCustomerId = $pdo->lastInsertId();
                $customerIds = range($firstCustomerId, $firstCustomerId + $totalRows - 1);

                // 3. Inserir registros em properties
                $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
                $sqlProperties = 'INSERT INTO properties (customer_id, unit, block, buy_value, outstanding_balance, created_at, updated_at) VALUES ' . $placeholders;
                $valuesProperties = [];
                foreach ($rows as $index => $row) {
                    $valuesProperties[] = $customerIds[$index];
                    $valuesProperties[] = $row['unit'];
                    $valuesProperties[] = $row['block'];
                    $valuesProperties[] = $row['buy_value'];
                    $valuesProperties[] = $row['outstanding_balance'];
                    $valuesProperties[] = $now;
                    $valuesProperties[] = $now;
                }
                $stmt = $pdo->prepare($sqlProperties);
                $stmt->execute($valuesProperties);

                // 4. Inserir registros em people (holders)
                $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
                $sqlPeople = 'INSERT INTO people (customer_id, name, phone, email, cpf, rg, rg_emitter, rg_issue_date, nationality, naturalness, mother_name, father_name, birthdate, marital_status, created_at, updated_at) VALUES ' . $placeholders;
                $valuesPeople = [];
                foreach ($rows as $index => $row) {
                    $valuesPeople[] = $customerIds[$index];
                    $valuesPeople[] = $row['name'];
                    $valuesPeople[] = $row['phone'];
                    $valuesPeople[] = $row['email'];
                    $valuesPeople[] = $row['cpf'];
                    $valuesPeople[] = $row['rg'];
                    $valuesPeople[] = $row['rg_emitter'];
                    $valuesPeople[] = $row['rg_issue_date'];
                    $valuesPeople[] = $row['nationality'];
                    $valuesPeople[] = $row['naturalness'];
                    $valuesPeople[] = $row['mother_name'];
                    $valuesPeople[] = $row['father_name'];
                    $valuesPeople[] = $row['birthdate'];
                    $valuesPeople[] = $row['marital_status'];
                    $valuesPeople[] = $now;
                    $valuesPeople[] = $now;
                }
                $stmt = $pdo->prepare($sqlPeople);
                $stmt->execute($valuesPeople);

                // Recupera os IDs de people inseridos
                $firstPeopleId = $pdo->lastInsertId();
                $peopleIds = range($firstPeopleId, $firstPeopleId + $totalRows - 1);

                // Cria um índice associativo para people usando o CPF
                $peopleIndex = [];
                foreach ($rows as $index => $row) {
                    $cpf = $row['cpf'];
                    $peopleIndex[$cpf] = [
                        'customer_id' => $customerIds[$index],
                        'person_id'   => $peopleIds[$index],
                    ];
                }

                // 5. Inserir registros em addresses
                $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
                $sqlAddresses = 'INSERT INTO addresses (person_id, address, number, complement, zipcode, district, city, state, created_at, updated_at) VALUES ' . $placeholders;
                $valuesAddresses = [];
                foreach ($rows as $index => $row) {
                    $valuesAddresses[] = $peopleIds[$index];
                    $valuesAddresses[] = $row['address'];
                    $valuesAddresses[] = $row['number'];
                    $valuesAddresses[] = $row['complement'];
                    $valuesAddresses[] = $row['zipcode'];
                    $valuesAddresses[] = $row['district'];
                    $valuesAddresses[] = $row['city'];
                    $valuesAddresses[] = $row['state'];
                    $valuesAddresses[] = $now;
                    $valuesAddresses[] = $now;
                }
                $stmt = $pdo->prepare($sqlAddresses);
                $stmt->execute($valuesAddresses);

                // 6. Inserir spouses para clientes casados
                $spousesRows = [];
                foreach ($rows as $row) {
                    if (strtolower($row['marital_status']) === strtolower(MaritalStatusEnum::Married->value)) {
                        if (isset($peopleIndex[$row['cpf']])) {
                            $spousesRows[] = $row;
                        }
                    }
                }
                if (!empty($spousesRows)) {
                    $totalSpouses = count($spousesRows);
                    $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalSpouses), ',');
                    $sqlSpouses = 'INSERT INTO people (customer_id, person_id, name, phone, email, cpf, rg, rg_emitter, rg_issue_date, created_at, updated_at) VALUES ' . $placeholders;
                    $valuesSpouses = [];
                    foreach ($spousesRows as $row) {
                        $cpf = $row['cpf'];
                        $customerId = $peopleIndex[$cpf]['customer_id'];
                        $personId = $peopleIndex[$cpf]['person_id'];
                        $valuesSpouses[] = $customerId;
                        $valuesSpouses[] = $personId;
                        $valuesSpouses[] = $row['c_name'];
                        $valuesSpouses[] = $row['c_phone'];
                        $valuesSpouses[] = $row['c_email'];
                        $valuesSpouses[] = $row['c_cpf'];
                        $valuesSpouses[] = $row['c_rg'];
                        $valuesSpouses[] = $row['c_rg_emitter'];
                        $valuesSpouses[] = $row['c_rg_issue_date'];
                        $valuesSpouses[] = $now;
                        $valuesSpouses[] = $now;
                    }
                    $stmt = $pdo->prepare($sqlSpouses);
                    $stmt->execute($valuesSpouses);
                }
            });
    }

    private function import07PDOPreparedChunked(string $filePath): void
    {
        $now = now()->format('Y-m-d H:i:s');
        $pdo = DB::connection()->getPdo();
        $handle = fopen($filePath, 'rb');
        // Pula o cabeçalho
        fgetcsv($handle);

        $chunkSize = 1000;
        $rows = [];

        try {
            while (($row = fgetcsv($handle)) !== false) {
                // Formatação das datas sem usar Carbon
                $data = explode('/', $row[10]);
                $rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

                $data = explode('/', $row[22]);
                $birthdate = "{$data[2]}-{$data[1]}-{$data[0]}";

                $data = explode('/', $row[30]);
                $c_rg_issue_date = "{$data[2]}-{$data[1]}-{$data[0]}";

                // Monta um array associativo com os dados do CSV
                $rows[] = [
                    'unit'                => $row[0],
                    'block'               => $row[1],
                    'buy_value'           => $row[2],
                    'outstanding_balance' => $row[3],
                    'name'                => $row[4],
                    'phone'               => $row[5],
                    'email'               => $row[6],
                    'cpf'                 => $row[7],
                    'rg'                  => $row[8],
                    'rg_emitter'          => $row[9],
                    'rg_issue_date'       => $rg_issue_date,
                    'address'             => $row[11],
                    'number'              => $row[12],
                    'complement'          => $row[13],
                    'zipcode'             => $row[14],
                    'district'            => $row[15],
                    'city'                => $row[16],
                    'state'               => $row[17],
                    'nationality'         => $row[18],
                    'naturalness'         => $row[19],
                    'mother_name'         => $row[20],
                    'father_name'         => $row[21],
                    'birthdate'           => $birthdate,
                    'marital_status'      => $row[23],
                    'c_name'              => $row[24],
                    'c_phone'             => $row[25],
                    'c_email'             => $row[26],
                    'c_cpf'               => $row[27],
                    'c_rg'                => $row[28],
                    'c_rg_emitter'        => $row[29],
                    'c_rg_issue_date'     => $c_rg_issue_date,
                    'created_at'          => $now,
                    'updated_at'          => $now,
                ];

                if (count($rows) === $chunkSize) {
                    $this->processChunkPdo($pdo, $rows, $now);
                    $rows = []; // reseta o chunk
                }
            }

            // Processa os registros restantes
            if (!empty($rows)) {
                $this->processChunkPdo($pdo, $rows, $now);
            }
        } finally {
            fclose($handle);
        }
    }

    private function import08Concurrent(string $filePath): void
    {
        $now = now()->format('Y-m-d H:i:s');
        $numberOfProcesses = 10;

        $tasks = [];
        for ($i = 0; $i < $numberOfProcesses; $i++) {
            $tasks[] = function () use ($filePath, $i, $numberOfProcesses, $now) {
                // Reconecta a cada processo para garantir uma conexão limpa
                DB::reconnect();
                $pdo = DB::connection()->getPdo();

                $handle = fopen($filePath, 'r');
                // Pula o cabeçalho
                fgetcsv($handle);
                $currentLine = 0;
                $chunk = [];

                // Define uma função anônima para processar um bloco (chunk) de registros
                $processChunk = function (array $chunk) use ($pdo, $now) {
                    $totalRows = count($chunk);

                    // 1. Inserir registros em customers
                    $placeholders = rtrim(str_repeat('(?, ?),', $totalRows), ',');
                    $sqlCustomers = 'INSERT INTO customers (created_at, updated_at) VALUES ' . $placeholders;
                    $valuesCustomers = [];
                    for ($j = 0; $j < $totalRows; $j++) {
                        $valuesCustomers[] = $now;
                        $valuesCustomers[] = $now;
                    }
                    $stmt = $pdo->prepare($sqlCustomers);
                    $stmt->execute($valuesCustomers);
                    $firstCustomerId = $pdo->lastInsertId();
                    $customerIds = range($firstCustomerId, $firstCustomerId + $totalRows - 1);

                    // 2. Inserir registros em properties
                    $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
                    $sqlProperties = 'INSERT INTO properties (customer_id, unit, block, buy_value, outstanding_balance, created_at, updated_at) VALUES ' . $placeholders;
                    $valuesProperties = [];
                    foreach ($chunk as $index => $row) {
                        $valuesProperties[] = $customerIds[$index];
                        $valuesProperties[] = $row['unit'];
                        $valuesProperties[] = $row['block'];
                        $valuesProperties[] = $row['buy_value'];
                        $valuesProperties[] = $row['outstanding_balance'];
                        $valuesProperties[] = $now;
                        $valuesProperties[] = $now;
                    }
                    $stmt = $pdo->prepare($sqlProperties);
                    $stmt->execute($valuesProperties);

                    // 3. Inserir registros em people (titular)
                    $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
                    $sqlPeople = 'INSERT INTO people (customer_id, name, phone, email, cpf, rg, rg_emitter, rg_issue_date, nationality, naturalness, mother_name, father_name, birthdate, marital_status, created_at, updated_at) VALUES ' . $placeholders;
                    $valuesPeople = [];
                    foreach ($chunk as $index => $row) {
                        $valuesPeople[] = $customerIds[$index];
                        $valuesPeople[] = $row['name'];
                        $valuesPeople[] = $row['phone'];
                        $valuesPeople[] = $row['email'];
                        $valuesPeople[] = $row['cpf'];
                        $valuesPeople[] = $row['rg'];
                        $valuesPeople[] = $row['rg_emitter'];
                        $valuesPeople[] = $row['rg_issue_date'];
                        $valuesPeople[] = $row['nationality'];
                        $valuesPeople[] = $row['naturalness'];
                        $valuesPeople[] = $row['mother_name'];
                        $valuesPeople[] = $row['father_name'];
                        $valuesPeople[] = $row['birthdate'];
                        $valuesPeople[] = $row['marital_status'];
                        $valuesPeople[] = $now;
                        $valuesPeople[] = $now;
                    }
                    $stmt = $pdo->prepare($sqlPeople);
                    $stmt->execute($valuesPeople);
                    $firstPeopleId = $pdo->lastInsertId();
                    $peopleIds = range($firstPeopleId, $firstPeopleId + $totalRows - 1);

                    // Cria um índice associativo para people usando o CPF
                    $peopleIndex = [];
                    foreach ($chunk as $index => $row) {
                        $cpf = $row['cpf'];
                        $peopleIndex[$cpf] = [
                            'customer_id' => $customerIds[$index],
                            'person_id'   => $peopleIds[$index],
                        ];
                    }

                    // 4. Inserir registros em addresses
                    $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
                    $sqlAddresses = 'INSERT INTO addresses (person_id, address, number, complement, zipcode, district, city, state, created_at, updated_at) VALUES ' . $placeholders;
                    $valuesAddresses = [];
                    foreach ($chunk as $index => $row) {
                        $valuesAddresses[] = $peopleIds[$index];
                        $valuesAddresses[] = $row['address'];
                        $valuesAddresses[] = $row['number'];
                        $valuesAddresses[] = $row['complement'];
                        $valuesAddresses[] = $row['zipcode'];
                        $valuesAddresses[] = $row['district'];
                        $valuesAddresses[] = $row['city'];
                        $valuesAddresses[] = $row['state'];
                        $valuesAddresses[] = $now;
                        $valuesAddresses[] = $now;
                    }
                    $stmt = $pdo->prepare($sqlAddresses);
                    $stmt->execute($valuesAddresses);

                    // 5. Inserir spouses para clientes casados
                    $spousesRows = [];
                    foreach ($chunk as $row) {
                        if (strtolower($row['marital_status']) === 'casado' && isset($peopleIndex[$row['cpf']])) {

                            $spousesRows[] = $row;
                        }
                    }
                    if (!empty($spousesRows)) {
                        $totalSpouses = count($spousesRows);
                        $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalSpouses), ',');
                        $sqlSpouses = 'INSERT INTO people (customer_id, person_id, name, phone, email, cpf, rg, rg_emitter, rg_issue_date, created_at, updated_at) VALUES ' . $placeholders;
                        $valuesSpouses = [];
                        foreach ($spousesRows as $row) {
                            $cpf = $row['cpf'];
                            $valuesSpouses[] = $peopleIndex[$cpf]['customer_id'];
                            $valuesSpouses[] = $peopleIndex[$cpf]['person_id'];
                            $valuesSpouses[] = $row['c_name'];
                            $valuesSpouses[] = $row['c_phone'];
                            $valuesSpouses[] = $row['c_email'];
                            $valuesSpouses[] = $row['c_cpf'];
                            $valuesSpouses[] = $row['c_rg'];
                            $valuesSpouses[] = $row['c_rg_emitter'];
                            $valuesSpouses[] = $row['c_rg_issue_date'];
                            $valuesSpouses[] = $now;
                            $valuesSpouses[] = $now;
                        }
                        $stmt = $pdo->prepare($sqlSpouses);
                        $stmt->execute($valuesSpouses);
                    }
                };

                // Lê o arquivo e distribui as linhas entre os processos (cada N-ésima linha)
                while (($line = fgetcsv($handle)) !== false) {
                    if ($currentLine++ % $numberOfProcesses !== $i) {
                        continue;
                    }
                    // Processa a linha convertendo as datas (índices 10, 22 e 30) de dd/mm/yyyy para yyyy-mm-dd
                    $row = [
                        'unit'                => $line[0],
                        'block'               => $line[1],
                        'buy_value'           => $line[2],
                        'outstanding_balance' => $line[3],
                        'name'                => $line[4],
                        'phone'               => $line[5],
                        'email'               => $line[6],
                        'cpf'                 => $line[7],
                        'rg'                  => $line[8],
                        'rg_emitter'          => $line[9],
                        'rg_issue_date'       => (function ($date) {
                            $parts = explode('/', $date);
                            return "{$parts[2]}-{$parts[1]}-{$parts[0]}";
                        })($line[10]),
                        'address'             => $line[11],
                        'number'              => $line[12],
                        'complement'          => $line[13],
                        'zipcode'             => $line[14],
                        'district'            => $line[15],
                        'city'                => $line[16],
                        'state'               => $line[17],
                        'nationality'         => $line[18],
                        'naturalness'         => $line[19],
                        'mother_name'         => $line[20],
                        'father_name'         => $line[21],
                        'birthdate'           => (function ($date) {
                            $parts = explode('/', $date);
                            return "{$parts[2]}-{$parts[1]}-{$parts[0]}";
                        })($line[22]),
                        'marital_status'      => $line[23],
                        'c_name'              => $line[24],
                        'c_phone'             => $line[25],
                        'c_email'             => $line[26],
                        'c_cpf'               => $line[27],
                        'c_rg'                => $line[28],
                        'c_rg_emitter'        => $line[29],
                        'c_rg_issue_date'     => (function ($date) {
                            $parts = explode('/', $date);
                            return "{$parts[2]}-{$parts[1]}-{$parts[0]}";
                        })($line[30]),
                        'created_at'          => $now,
                        'updated_at'          => $now,
                    ];

                    $chunk[] = $row;

                    // Quando o chunk atingir 1000 registros, processa-o e limpa o array
                    if (count($chunk) >= 1000) {
                        $processChunk($chunk);
                        $chunk = [];
                    }
                }

                // Processa o chunk final, se houver
                if (!empty($chunk)) {
                    $processChunk($chunk);
                }

                fclose($handle);

                return true;
            };
        }

        Concurrency::run($tasks);
    }

    private function validateImport(string $filePath): void
    {
        $now = now()->format('Y-m-d H:i:s');
        $numberOfProcesses = 10;

        // Define o arquivo de log para erros (evite usar caracteres inválidos no nome do arquivo)
        $errorLogFile =$filePath .'.log';

        $tasks = [];
        for ($i = 0; $i < $numberOfProcesses; $i++) {
            $tasks[] = function () use ($filePath, $i, $numberOfProcesses, $now, $errorLogFile) {
                // Reconecta a cada processo para garantir uma conexão limpa
                DB::reconnect();
                $pdo = DB::connection()->getPdo();

                $handle = fopen($filePath, 'r');
                // Pula o cabeçalho
                fgetcsv($handle);
                $currentLine = 1; // Contador de linha (já pulou o cabeçalho)
                $chunk = [];

                // Função para processar um bloco (chunk) de registros
                $processChunk = function (array $chunk) use ($pdo, $now, $errorLogFile) {

                    $validatedemails = [];

                    foreach ($chunk as $row) {
                        $validator = \Validator::make($row, [
                            'unit'                => 'required',
                            'block'               => 'required',
                            'buy_value'           => 'required',
                            'outstanding_balance' => 'required',
                            'name'                => 'required|min:3|max:255',
                            'phone'               => 'required',
                            'email'               => 'required|email',
                            'cpf'                 => 'required',
                            'rg'                  => 'required',
                            'rg_emitter'          => 'required',
                            'rg_issue_date'       => 'required',
                            'address'             => 'required',
                            'number'              => 'required',
                            'complement'          => 'nullable',
                            'zipcode'             => 'required',
                            'district'            => 'required',
                            'city'                => 'required',
                            'state'               => 'required',
                            'nationality'         => 'required',
                            'naturalness'         => 'required',
                            'mother_name'         => 'required',
                            'father_name'         => 'required',
                            'birthdate'           => 'required',
                            'marital_status'      => 'required',
                            'c_name'              => 'required',
                            'c_phone'             => 'required',
                            'c_email'             => 'required',
                            'c_cpf'               => 'required',
                            'c_rg'                => 'required',
                            'c_rg_emitter'        => 'required',
                            'c_rg_issue_date'     => 'required',
                        ]);

                        if ($validator->fails()) {
                            // Recupera os erros e formata a mensagem
                            $errors = implode(', ', $validator->errors()->all());
                            $errorMessage = "Linha {$row['line_number']}: {$errors}\n";
                            // Escreve a mensagem no arquivo de log com bloqueio para evitar conflitos em processos concorrentes
                            file_put_contents($errorLogFile, $errorMessage, FILE_APPEND | LOCK_EX);
                            continue;
                        }

                        $validatedEmails[] = [
                            'email'       => $row['email'],
                            'line_number' => $row['line_number']
                        ];
                    }

                    if (!empty($validatedEmails)) {
                        $emails = array_column($validatedEmails, 'email');
                        $existingEmails = Person::whereIn('email', $emails)->pluck('email')->toArray();

                        // Para cada email validado, se já existir, registra no log
                        foreach ($validatedEmails as $entry) {
                            if (in_array($entry['email'], $existingEmails)) {
                                $errorMessage = "Linha {$entry['line_number']}: Email {$entry['email']} já existe no banco de dados.\n";
                                file_put_contents($errorLogFile, $errorMessage, FILE_APPEND | LOCK_EX);
                                // Se necessário, remova ou marque este registro para que não seja inserido
                            } else {
                                // Aqui você pode inserir o registro no banco de dados
                                // Exemplo: Person::create($row);
                            }
                        }
                    }

                };

                while (($line = fgetcsv($handle)) !== false) {
                    // Distribui as linhas entre os processos
                    if ($currentLine++ % $numberOfProcesses !== $i) {
                        continue;
                    }

                    // Adiciona o número da linha para facilitar o log
                    $row = [
                        'line_number'         => $currentLine,
                        'unit'                => $line[0],
                        'block'               => $line[1],
                        'buy_value'           => $line[2],
                        'outstanding_balance' => $line[3],
                        'name'                => $line[4],
                        'phone'               => $line[5],
                        'email'               => $line[6],
                        'cpf'                 => $line[7],
                        'rg'                  => $line[8],
                        'rg_emitter'          => $line[9],
                        'rg_issue_date'       => (function ($date) {
                            $parts = explode('/', $date);
                            return "{$parts[2]}-{$parts[1]}-{$parts[0]}";
                        })($line[10]),
                        'address'             => $line[11],
                        'number'              => $line[12],
                        'complement'          => $line[13],
                        'zipcode'             => $line[14],
                        'district'            => $line[15],
                        'city'                => $line[16],
                        'state'               => $line[17],
                        'nationality'         => $line[18],
                        'naturalness'         => $line[19],
                        'mother_name'         => $line[20],
                        'father_name'         => $line[21],
                        'birthdate'           => (function ($date) {
                            $parts = explode('/', $date);
                            return "{$parts[2]}-{$parts[1]}-{$parts[0]}";
                        })($line[22]),
                        'marital_status'      => $line[23],
                        'c_name'              => $line[24],
                        'c_phone'             => $line[25],
                        'c_email'             => $line[26],
                        'c_cpf'               => $line[27],
                        'c_rg'                => $line[28],
                        'c_rg_emitter'        => $line[29],
                        'c_rg_issue_date'     => (function ($date) {
                            $parts = explode('/', $date);
                            return "{$parts[2]}-{$parts[1]}-{$parts[0]}";
                        })($line[30]),
                        'created_at'          => $now,
                        'updated_at'          => $now,
                    ];

                    $chunk[] = $row;

                    if (count($chunk) >= 1000) {
                        $processChunk($chunk);
                        $chunk = [];
                    }
                }

                if (!empty($chunk)) {
                    $processChunk($chunk);
                }

                fclose($handle);

                return true;
            };
        }

        Concurrency::run($tasks);
    }

    private function processChunkPdo(PDO $pdo, array $rows, string $now): void
    {
        $totalRows = count($rows);

        // 1. Inserir registros em customers (somente created_at e updated_at)
        $placeholders = rtrim(str_repeat('(?, ?),', $totalRows), ',');
        $sqlCustomers = "INSERT INTO customers (created_at, updated_at) VALUES {$placeholders}";
        $valuesCustomers = [];
        for ($i = 0; $i < $totalRows; $i++) {
            $valuesCustomers[] = $now;
            $valuesCustomers[] = $now;
        }
        $stmt = $pdo->prepare($sqlCustomers);
        $stmt->execute($valuesCustomers);
        $firstCustomerId = $pdo->lastInsertId();
        $customerIds = range($firstCustomerId, $firstCustomerId + $totalRows - 1);

        // 2. Inserir registros em properties
        $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
        $sqlProperties = "INSERT INTO properties (customer_id, unit, block, buy_value, outstanding_balance, created_at, updated_at) VALUES {$placeholders}";
        $valuesProperties = [];
        foreach ($rows as $index => $row) {
            $valuesProperties[] = $customerIds[$index];
            $valuesProperties[] = $row['unit'];
            $valuesProperties[] = $row['block'];
            $valuesProperties[] = $row['buy_value'];
            $valuesProperties[] = $row['outstanding_balance'];
            $valuesProperties[] = $now;
            $valuesProperties[] = $now;
        }
        $stmt = $pdo->prepare($sqlProperties);
        $stmt->execute($valuesProperties);

        // 3. Inserir registros em people (holders)
        $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
        $sqlPeople = "INSERT INTO people (customer_id, name, phone, email, cpf, rg, rg_emitter, rg_issue_date, nationality, naturalness, mother_name, father_name, birthdate, marital_status, created_at, updated_at)
                  VALUES {$placeholders}";
        $valuesPeople = [];
        foreach ($rows as $index => $row) {
            $valuesPeople[] = $customerIds[$index];
            $valuesPeople[] = $row['name'];
            $valuesPeople[] = $row['phone'];
            $valuesPeople[] = $row['email'];
            $valuesPeople[] = $row['cpf'];
            $valuesPeople[] = $row['rg'];
            $valuesPeople[] = $row['rg_emitter'];
            $valuesPeople[] = $row['rg_issue_date'];
            $valuesPeople[] = $row['nationality'];
            $valuesPeople[] = $row['naturalness'];
            $valuesPeople[] = $row['mother_name'];
            $valuesPeople[] = $row['father_name'];
            $valuesPeople[] = $row['birthdate'];
            $valuesPeople[] = $row['marital_status'];
            $valuesPeople[] = $now;
            $valuesPeople[] = $now;
        }
        $stmt = $pdo->prepare($sqlPeople);
        $stmt->execute($valuesPeople);
        $firstPeopleId = $pdo->lastInsertId();
        $peopleIds = range($firstPeopleId, $firstPeopleId + $totalRows - 1);

        // 4. Criar índice associativo para people (usando o cpf)
        $peopleIndex = [];
        foreach ($rows as $index => $row) {
            $cpf = $row['cpf'];
            $peopleIndex[$cpf] = [
                'customer_id' => $customerIds[$index],
                'person_id'   => $peopleIds[$index],
            ];
        }

        // 5. Inserir registros em addresses
        $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalRows), ',');
        $sqlAddresses = "INSERT INTO addresses (person_id, address, number, complement, zipcode, district, city, state, created_at, updated_at)
                     VALUES {$placeholders}";
        $valuesAddresses = [];
        foreach ($rows as $index => $row) {
            $valuesAddresses[] = $peopleIds[$index];
            $valuesAddresses[] = $row['address'];
            $valuesAddresses[] = $row['number'];
            $valuesAddresses[] = $row['complement'];
            $valuesAddresses[] = $row['zipcode'];
            $valuesAddresses[] = $row['district'];
            $valuesAddresses[] = $row['city'];
            $valuesAddresses[] = $row['state'];
            $valuesAddresses[] = $now;
            $valuesAddresses[] = $now;
        }
        $stmt = $pdo->prepare($sqlAddresses);
        $stmt->execute($valuesAddresses);

        // 6. Inserir spouses para clientes casados
        $spousesRows = [];
        foreach ($rows as $row) {
            // Verifica se o status indica casamento (supondo a existência de MaritalStatusEnum::Married)
            if (strtolower($row['marital_status']) === strtolower(MaritalStatusEnum::Married->value)) {
                if (isset($peopleIndex[$row['cpf']])) {
                    $spousesRows[] = $row;
                }
            }
        }
        if (!empty($spousesRows)) {
            $totalSpouses = count($spousesRows);
            $placeholders = rtrim(str_repeat('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),', $totalSpouses), ',');
            $sqlSpouses = "INSERT INTO people (customer_id, person_id, name, phone, email, cpf, rg, rg_emitter, rg_issue_date, created_at, updated_at)
                       VALUES {$placeholders}";
            $valuesSpouses = [];
            foreach ($spousesRows as $row) {
                $cpf = $row['cpf'];
                $customerId = $peopleIndex[$cpf]['customer_id'];
                $personId = $peopleIndex[$cpf]['person_id'];
                $valuesSpouses[] = $customerId;
                $valuesSpouses[] = $personId;
                $valuesSpouses[] = $row['c_name'];
                $valuesSpouses[] = $row['c_phone'];
                $valuesSpouses[] = $row['c_email'];
                $valuesSpouses[] = $row['c_cpf'];
                $valuesSpouses[] = $row['c_rg'];
                $valuesSpouses[] = $row['c_rg_emitter'];
                $valuesSpouses[] = $row['c_rg_issue_date'];
                $valuesSpouses[] = $now;
                $valuesSpouses[] = $now;
            }
            $stmt = $pdo->prepare($sqlSpouses);
            $stmt->execute($valuesSpouses);
        }
    }
}
