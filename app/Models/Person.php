<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\Relations\HasOne;

class Person extends Model
{
    protected $fillable = [
        'customer_id',
        'person_id',
        'type_id',
        'name',
        'email',
        'phone',
        'cpf',
        'rg',
        'rg_emitter',
        'rg_issue_date',
        'nationality',
        'naturalness',
        'mother_name',
        'father_name',
        'birthdate',
        'marital_status',
    ];

    public function type(): BelongsTo
    {
        return $this->belongsTo(Type::class);
    }

    public function customer(): BelongsTo
    {
        return $this->belongsTo(Customer::class);
    }

    public function spouse(): HasOne
    {
        return $this->hasOne(Person::class, 'person_id', 'id');
    }

    public function addresses(): HasMany
    {
        return $this->hasMany(Address::class);
    }
}
