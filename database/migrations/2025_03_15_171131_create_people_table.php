<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('people', function (Blueprint $table) {
            $table->id();
            $table->foreignId('customer_id')->constrained('customers')->onDelete('cascade');
            $table->foreignId('person_id')->nullable()->constrained('people');
            $table->foreignId('type_id')->nullable()->constrained('types');
            $table->string('name');
            $table->string('email')->unique();
            $table->string('phone');
            $table->string('cpf');
            $table->string('rg');
            $table->string('rg_emitter')->nullable();
            $table->string('rg_issue_date')->nullable();
            $table->string('nationality')->nullable();
            $table->string('naturalness')->nullable();
            $table->string('mother_name')->nullable();
            $table->string('father_name')->nullable();
            $table->string('birthdate')->nullable();
            $table->string('marital_status')->nullable();

            $table->timestamps();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('people');
    }
};
