<?php

namespace Database\Seeders;

use App\Models\Type;
use App\Models\User;
// use Illuminate\Database\Console\Seeds\WithoutModelEvents;
use Illuminate\Database\Seeder;

class DatabaseSeeder extends Seeder
{
    /**
     * Seed the application's database.
     */
    public function run(): void
    {
        Type::create(['name' => 'Titular']);
        Type::create(['name' => 'Conjuge']);
    }
}
